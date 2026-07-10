package PPI::Transform::Script;

use strict;
use warnings;
use parent 'PPI::Transform';
use PPI;

sub document {
    my ($self, $doc) = @_;
    my $changes = 0;

    # 1. Idempotency check: Skip if $script is already defined
    return 0 if $doc->find_first(sub { 
        $_[1]->isa('PPI::Statement') && $_[1]->content =~ /\$script\s*=/ 
    });

    # 2. Find the anchor for the Constructor
    # We want the LAST statement that assigns to either $logger or $config
    my $constructor_anchor;
    my $statements = $doc->find('PPI::Statement') || [];
    
    foreach my $stmt (@$statements) {
        # Matches $logger = ... or $config = ...
        if ($stmt->content =~ /\$(logger|config)\s*=[^=>]/) {
            $constructor_anchor = $stmt;
        }
    }

    # 3. Insert the Constructor if we found our variables
    if ($constructor_anchor) {
        my $new_code = <<'END_CONSTRUCTOR';

#
#
#
my $script = Mx::PerlScript->new( logger => $logger, config => $config );
END_CONSTRUCTOR

        my $new_nodes = PPI::Document->new(\$new_code);
        my $current_anchor = $constructor_anchor;
        
        while (my $node = $new_nodes->first_element) {
            $new_nodes->remove_child($node);
            $current_anchor->insert_after($node);
            $current_anchor = $node;
        }
        $changes++;
    }

    # 4. Insert $script->finish() at the end of the script
    # We check if it's already there first
    unless ($doc->find_first(sub { $_[1]->content =~ /\$script->finish/ })) {
        
        # Find the last "significant" element before __END__ or __DATA__
        my $end_anchor = $doc->find_first(sub { $_[1]->isa('PPI::Statement::End') })
                      || $doc->find_first(sub { $_[1]->isa('PPI::Statement::Data') });

        my $finish_code = "\n\$script->finish();\n";
        my $finish_doc = PPI::Document->new(\$finish_code);

        # Transfer every element (leading whitespace, the statement
        # itself, trailing whitespace), not just first_element() --
        # that's only the mini-document's first *child* (the leading
        # Whitespace token), which would silently drop the actual
        # "$script->finish();" statement and leave a stray blank line.
        while (my $node = $finish_doc->first_element) {
            $finish_doc->remove_child($node);
            if ($end_anchor) {
                # Insert before __END__
                $end_anchor->insert_before($node);
            } else {
                # Insert at the very end of the file
                $doc->add_element($node);
            }
        }
        $changes++;
    }

    return $changes;
}

1;