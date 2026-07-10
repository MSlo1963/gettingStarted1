package PPI::Transform::RemoveAccountCreation;

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

    # 1. Find all occurrences of "Mx::Account"
    my $words = $doc->find(sub {
        $_[1]->isa('PPI::Token::Word') && $_[1]->content eq 'Mx::Account'
    });

    return 0 unless $words;

    # Use a hash to ensure we only process each statement once
    my %statements_to_delete;

    foreach my $word (@$words) {
        # Get the top-level statement containing this word
        my $stmt = $word->statement;
        
        # Check if this statement involves a 'new' constructor
        if ($stmt && $stmt->content =~ /\bnew\b/) {
            # Use the object reference as the key to avoid the "uninitialized" error
            $statements_to_delete{$stmt} = $stmt;
        }
    }

    # Process unique statements
    foreach my $stmt (values %statements_to_delete) {
        
        # 2. Look backwards for "attached" comments. A blank line marks
        # where "attached" ends. A Comment token already embeds its own
        # trailing newline (unlike a Statement), so it only takes one
        # *more* newline in the whitespace above it to make a blank
        # line, versus two after a Statement. PPI can also split what
        # is visually one blank line into multiple single-newline
        # Whitespace tokens in a row, so newlines are accumulated
        # across the whole contiguous whitespace run rather than
        # checked one token's content at a time.
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
            else {
                last; # Hit other code
            }
        }

        # 3. Execute Deletions
        # Delete attached comments
        foreach my $node (@to_remove) {
            $node->delete;
        }

        # Delete the trailing newline of the statement itself
        my $next = _next_sibling($stmt);
        if ($next && $next->isa('PPI::Token::Whitespace')) {
            $next->delete;
        }

        # Delete the code line
        $stmt->delete;
        $changes++;
    }

    return $changes;
}

1;