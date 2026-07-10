package PPI::Transform::AddAuthor;
use strict;
use warnings;
use parent 'PPI::Transform';

# This is the "Engine Room" - like a main XSLT template
sub document {
    my ($self, $doc) = @_;
    my $changes = 0;

    # 1. Find all Subroutine statements
    my $subs = $doc->find('PPI::Statement::Sub');
    return 0 unless $subs;

    foreach my $sub (@$subs) {
        # Don't stack up duplicate markers if this transform already ran
        # on this file (skip whitespace back to the nearest real sibling).
        my $prev = $sub->previous_sibling;
        $prev = $prev->previous_sibling
            while $prev && $prev->isa('PPI::Token::Whitespace');
        next if $prev
            && $prev->isa('PPI::Token::Comment')
            && $prev->content =~ /^# --- Refactored by PPI ---/;

        # 2. Create a new comment token
        my $comment = PPI::Token::Comment->new("# --- Refactored by PPI ---\n");

        # 3. Insert it before the subroutine
        $sub->insert_before($comment);
        $changes++;
    }

    # Return number of changes made
    return $changes;
}

1;