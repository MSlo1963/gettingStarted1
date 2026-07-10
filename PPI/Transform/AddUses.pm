package PPI::Transform::AddUses;
use strict;
use warnings;
use parent 'PPI::Transform';
use PPI;
use Scalar::Util qw(refaddr);

my @REQUIRED = qw( Mx::DB Mx::PerlScript Mx::SQLLibrary Data::Dumper );

sub document {
    my ($self, $doc) = @_;

    # 1. Walk the top-level "header": an optional shebang, followed by a
    # run of 'use' statements and blank lines. Stop at the first element
    # that is neither -- that's where real content (code/comments) starts,
    # and it's what we anchor the rebuilt header on.
    my @kids = $doc->children;
    my $has_shebang = @kids && $kids[0]->isa('PPI::Token::Comment') && $kids[0]->content =~ /^#!/;
    my $idx = $has_shebang ? 1 : 0;

    my %final_modules;
    my @header;
    my @existing_order;
    my $header_has_blank_line = 0;
    while ($idx < @kids) {
        my $el = $kids[$idx];
        if ($el->isa('PPI::Statement::Include')) {
            my $mod = $el->module;
            if ($mod) {
                $final_modules{$mod} = 1;
                push @existing_order, $mod;
            }
            push @header, $el;
        } elsif ($el->isa('PPI::Token::Whitespace')) {
            $header_has_blank_line = 1 if $el->content =~ /\n\s*\n/;
            push @header, $el;
        } else {
            last;
        }
        $idx++;
    }
    my $anchor = $kids[$idx];  # first real content after the header, if any

    # 2. A required module might already be use'd somewhere outside the
    # contiguous header block (e.g. after a comment that breaks the
    # header run, or further down the file) -- check the whole document,
    # not just @header, so we don't add a duplicate 'use' for it.
    my %used_elsewhere;
    my %header_addrs = map { (refaddr($_) => 1) } @header;
    my $all_includes = $doc->find('PPI::Statement::Include') || [];
    foreach my $inc (@$all_includes) {
        next if $header_addrs{ refaddr($inc) };
        my $mod = $inc->module;
        $used_elsewhere{$mod} = 1 if $mod;
    }

    # 3. Add the new required modules to our list, counting only genuinely
    # new additions as "changes" -- every existing use is kept regardless.
    my $added = 0;
    foreach my $req (@REQUIRED) {
        next if $final_modules{$req};   # already in the header
        next if $used_elsewhere{$req};  # already use'd further down -- don't duplicate it
        $final_modules{$req} = 1;
        $added++;
    }

    # 4. Create the new consolidated string (sorted looks best). We put
    # 'strict' and 'warnings' first if they exist. A single trailing blank
    # line separates the header from whatever real content follows.
    my @sorted = sort keys %final_modules;
    my @canonical_order = ((grep { $final_modules{$_} } qw(strict warnings)),
                            grep { $_ ne 'strict' && $_ ne 'warnings' } @sorted);

    # Nothing to do: every required module is already present, already in
    # canonical order, and there's no stray blank line inside the header
    # to clean up -- rebuilding would just reproduce the same text.
    if (!$added && !$header_has_blank_line
        && join("\0", @existing_order) eq join("\0", @canonical_order)) {
        return 0;
    }

    # 5. Clear out the old header (old uses + the blank lines between/
    # around them) -- we rebuild it from scratch so nothing is left behind.
    $_->delete for @header;

    my $new_code = '';
    foreach my $m (@canonical_order) {
        $new_code .= "use $m;\n";
    }
    $new_code .= "\n" if $anchor;

    # 6. Splice the rebuilt header in after the shebang (or at the very
    # top if there wasn't one). We insert directly via the Node-level
    # __insert_before_child()/__insert_after_child() primitives instead
    # of the public insert_before()/insert_after() methods: those refuse
    # to place a PPI::Statement next to a PPI::Token (e.g. the shebang,
    # or a lone comment used as $anchor here), which is exactly the shape
    # of a 'use' statement next to a comment -- the public API can't
    # express this splice at all, even though it's structurally valid.
    my $mini = PPI::Document->new(\$new_code);
    my @new_elements = map { $_->clone } $mini->children;

    if ($anchor) {
        $doc->__insert_before_child($anchor, @new_elements);
    } elsif ($has_shebang) {
        # Header ran to the end of the document, but there's a shebang
        # (still present at $kids[0], untouched by the deletions above)
        # to insert after.
        $doc->__insert_after_child($kids[0], @new_elements);
    } else {
        # No shebang, no anchor -- document was empty or entirely blanks.
        $doc->add_element($_) for @new_elements;
    }

    return $added || 1;
}
1;
