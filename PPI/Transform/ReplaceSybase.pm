package PPI::Transform::ReplaceSybase;

use strict;
use warnings;
use parent 'PPI::Transform';
use PPI;
use Scalar::Util qw(refaddr);

# PPI::Element's previous_sibling()/next_sibling() rely on an internal
# refaddr-keyed position cache (PPI::Node::__position) that can go stale
# after another transform (e.g. AddUses) has spliced a batch of new
# elements into this same document earlier in the pipeline, causing them
# to return the wrong node entirely. Walk the parent's live children()
# list instead, which always reflects the actual current tree.
sub _prev_sibling {
    my ($node) = @_;
    my $parent = $node->parent or return undef;
    my @kids = $parent->children;
    for my $i (0 .. $#kids) {
        next unless refaddr($kids[$i]) == refaddr($node);
        return $i > 0 ? $kids[$i - 1] : undef;
    }
    return undef;
}

sub _next_sibling {
    my ($node) = @_;
    my $parent = $node->parent or return undef;
    my @kids = $parent->children;
    for my $i (0 .. $#kids) {
        next unless refaddr($kids[$i]) == refaddr($node);
        return $i < $#kids ? $kids[$i + 1] : undef;
    }
    return undef;
}

sub document {
    my ($self, $doc) = @_;
    my $changes = 0;
    my @var_pairs;  # [old_var_name, new_var_name] per replacement made

    # 1. Find all occurrences of "Mx::Sybase"
    my $words = $doc->find(sub {
        $_[1]->isa('PPI::Token::Word') && $_[1]->content eq 'Mx::Sybase'
    });

    return 0 unless $words;

    my %processed_statements;

    foreach my $word (@$words) {
        my $stmt = $word->statement;
        next unless $stmt;
        next if $processed_statements{$stmt}++;

        # Verify it's a constructor with the specific database config
        my $content = $stmt->content;
        my ($db_config) = $content =~ /database\s*=>\s*\$config->(DB_NAME|DB_REP)\b/i;
        if ($content =~ /\bnew\b/ && $db_config) {
            my $db_role = uc($db_config) eq 'DB_REP' ? 'REP' : 'FIN';

            # 2. Capture the variable name being assigned (the LHS)
            # We look for the first symbol (e.g. $db) in the statement
            my $var_node = $stmt->find_first('PPI::Token::Symbol');
            my $old_var_name = $var_node ? $var_node->content : undef;
            my $var_name = uc($db_config) eq 'DB_REP' ? '$rep_db_compat_sybase' : '$fin_db_compat_sybase';

            # 3. Find attached comments (preceding). A blank line marks
            # where "attached" ends. A Comment token already embeds its
            # own trailing newline (unlike a Statement), so it only
            # takes one *more* newline in the whitespace above it to
            # make a blank line, versus two after a Statement. PPI can
            # also split what is visually one blank line into multiple
            # single-newline Whitespace tokens in a row, so newlines
            # are accumulated across the whole contiguous whitespace
            # run rather than checked one token's content at a time.
            my @to_remove;
            my $current = _prev_sibling($stmt);
            my $ws_newlines = 0;
            my $blank_line_threshold = 2;
            while ($current) {
                if ($current->isa('PPI::Token::Whitespace')) {
                    $ws_newlines += ($current->content =~ tr/\n//);
                    last if $ws_newlines >= $blank_line_threshold;
                    unshift @to_remove, $current;
                    $current = _prev_sibling($current);
                }
                elsif ($current->isa('PPI::Token::Comment')) {
                    unshift @to_remove, $current;
                    $current = _prev_sibling($current);
                    $ws_newlines = 0;
                    $blank_line_threshold = 1;
                }
                else { last; }
            }

            # 4. Define the new code (Heredoc style)
            # We use the captured $var_name here
            my $replacement_code = <<"END_NEW_CODE";

#
#
#
$var_name = Mx::DB->new(
    config       => \$config,
    logger       => \$logger,
    auto_account => 1,
    db_role      => '$db_role',
    compat       => 'sybase',
);
unless ($var_name) {
    \$script->fail_and_die("exception - Mx::DB->new() failed");
}
END_NEW_CODE

            # 5. Execute the swap
            # First, remove comments
            $_->delete for @to_remove;

            # Create the new block
            my $new_doc = PPI::Document->new(\$replacement_code);

            # Insert the new nodes before the old statement
            my $anchor = $stmt;
            while (my $new_node = $new_doc->first_element) {
                $new_doc->remove_child($new_node);
                $anchor->insert_before($new_node);
            }

            # Delete the original statement and its trailing newline
            my $next = _next_sibling($stmt);
            if ($next && $next->isa('PPI::Token::Whitespace')) { $next->delete; }
            $stmt->delete;

            # Any other reference to the old variable elsewhere in the
            # document (e.g. "$sybase->open()") would otherwise be left
            # dangling/undeclared once the declaration above is gone --
            # rename those to the new variable too.
            if (defined $old_var_name && $old_var_name ne $var_name) {
                my $other_refs = $doc->find(sub {
                    $_[1]->isa('PPI::Token::Symbol') && $_[1]->content eq $old_var_name
                });
                $_->set_content($var_name) for @{ $other_refs || [] };
            }

            $changes++;
            push @var_pairs, [$old_var_name, $var_name];
        }
    }

    return ($changes, @var_pairs);
}

1;
