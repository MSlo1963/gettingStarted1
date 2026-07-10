package PPI::Transform::RemoveObsoleteUses;
use strict;
use warnings;
use parent 'PPI::Transform';

my %REMOVABLES = map { $_ => 1 } qw( Mx::Sybase Mx::Sybase2 Mx::Account );

sub document {
    my ($self, $doc) = @_;
    my $includes = $doc->find('PPI::Statement::Include') or return 0;
    my $changes = 0;

    foreach my $include (@$includes) {
        my $module = eval { $include->module };
        if ($module && $REMOVABLES{$module}) {
            my $next = $include->next_sibling;
            $include->delete;
            # Clean up the newline
            if ($next && $next->isa('PPI::Token::Whitespace')) { $next->delete; }
            $changes++;
        }
    }
    return $changes;
}
1;