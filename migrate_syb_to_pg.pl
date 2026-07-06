#!/usr/bin/perl
use strict;
use warnings;
use FindBin;
use PPI;

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
my %db_name_map = (
    FIN_DB => 'FIN',
    HR_DB  => 'HR',
);

# Real scripts pass database => $config->SOME_ACCESSOR rather than a literal
# string. Map the accessor name to the db_role value Mx::DB expects.
my %accessor_role_map = (
    DB_NAME => 'FIN',
    DB_REP  => 'REP',
);

my $OLD_MODULE = 'Mx::Sybase2';
my $NEW_MODULE = 'Mx::DB';
my $FLAVOUR    = 'sybase2';   # used as the `compat` value in the new call

die "Usage: $0 <file.pl>\n" unless @ARGV;
my $file = shift @ARGV;

my $doc = PPI::Document->new($file) or die "Could not parse $file: $!\n";
$doc->index_locations;  # ensure ->location is populated on every element

# ---------------------------------------------------------------------
# Helper: compare two [line, col, ...] location arrays
# returns -1, 0, 1 like <=>
# ---------------------------------------------------------------------
sub loc_cmp {
    my ($a, $b) = @_;
    return $a->[0] <=> $b->[0] || $a->[1] <=> $b->[1];
}

# ---------------------------------------------------------------------
# Helper: is $node anywhere inside $container (including being $container
# itself)? Used to exclude tokens that belong to the declaration statement
# being replaced, so they don't get double-counted as "renamed references".
# ---------------------------------------------------------------------
sub is_within {
    my ($node, $container) = @_;
    while ($node) {
        return 1 if $node == $container;
        $node = $node->parent;
    }
    return 0;
}

# ---------------------------------------------------------------------
# Helper: find the nearest enclosing scope root for a statement.
# A scope root is either a PPI::Structure::Block (sub body, if/while/for
# body, bare block) or the PPI::Document itself for file-level 'my's.
# ---------------------------------------------------------------------
sub find_scope_root {
    my ($stmt) = @_;
    my $node = $stmt->parent;
    while ($node) {
        return $node if $node->isa('PPI::Structure::Block');
        return $node if $node->isa('PPI::Document');
        $node = $node->parent;
    }
    return $stmt->top;
}

# ---------------------------------------------------------------------
# Helper: is $symbol shadowed by an inner 'my' redeclaration of the same
# name, somewhere between $scope_root and $symbol?
# ---------------------------------------------------------------------
sub is_shadowed {
    my ($symbol, $bare_name, $scope_root, $original_decl) = @_;

    my $node = $symbol->parent;
    while ($node && $node != $scope_root) {
        if ($node->isa('PPI::Structure::Block') || $node->isa('PPI::Document')) {
            my $decls = $node->find(sub {
                my (undef, $n) = @_;
                return 0 unless $n->isa('PPI::Statement::Variable');
                return 0 unless $n->type eq 'my';
                return grep { $_ eq "\$$bare_name" } $n->variables;
            }) || [];

            for my $d (@$decls) {
                next if $d == $original_decl;
                next if loc_cmp($d->location, $symbol->location) > 0;
                return 1;  # found a closer, shadowing declaration
            }
        }
        $node = $node->parent;
    }
    return 0;
}

# ---------------------------------------------------------------------
# PHASE A: find every "my $x = Mx::Sybase2->new(...)" declaration,
# and pre-compute the new name, scope root, in-scope symbols, and
# replacement text -- without mutating anything yet.
# ---------------------------------------------------------------------
my @plan;

my $var_decls = $doc->find(sub {
    my (undef, $n) = @_;
    return 0 unless $n->isa('PPI::Statement::Variable');
    return 0 unless $n->type eq 'my';
    return $n->content =~ /\Q$OLD_MODULE\E\s*->\s*new\b/;
}) || [];

for my $stmt (@$var_decls) {
    my ($old_var_name) = $stmt->variables;
    next unless $old_var_name;
    (my $bare_old_name = $old_var_name) =~ s/^\$//;

    my ($arg_list) = grep { $_->isa('PPI::Structure::List') } $stmt->schildren;
    my %args;
    if ($arg_list) {
        my $arg_text = $arg_list->content;
        while ($arg_text =~ /(\w+)\s*=>?\s*('[^']*'|\$\w+(?:\s*->\s*\w+)*)/g) {
            $args{$1} = $2;
        }
    }

    my $db_arg = $args{database} // '';
    my ($db_role_expr, $new_var_name, $role_resolved);
    if ($db_arg =~ /^\$\w+\s*->\s*(\w+)$/) {
        # database => $config->SOME_ACCESSOR -- map the accessor name to a
        # db_role. Can't be resolved statically if it's not in the table.
        my $accessor = $1;
        if (my $role = $accessor_role_map{$accessor}) {
            $db_role_expr  = "'$role'";
            $new_var_name  = '$' . lc($role) . '_db';
            $role_resolved = 1;
        }
        else {
            $db_role_expr = $db_arg;
            $new_var_name = '$' . $bare_old_name . '_db';
        }
    }
    elsif ($db_arg =~ /^'(.*)'$/) {
        my $literal = $1;
        my $mapped_db = $db_name_map{$literal} // $literal;
        $db_role_expr  = "'$mapped_db'";
        $new_var_name  = '$' . lc($mapped_db) . '_db';
        $role_resolved = 1;
    }
    else {
        # Unresolvable expression (bare variable, missing, etc.) -- preserve
        # it as-is and fall back to a name derived from the old variable.
        $db_role_expr = $db_arg || 'undef';
        $new_var_name = '$' . $bare_old_name . '_db';
    }

    my $config_expr = $args{config} // '$config';
    my $logger_expr = $args{logger} // '$logger';

    my $new_text = sprintf(
        "my %s;\n" .
        "unless (\n" .
        "    %s = %s->new(\n" .
        "        config => %s,\n" .
        "        logger => %s,\n" .
        "        auto_account => 1,\n" .
        "        db_role => %s,\n" .
        "        compat => '%s')\n" .
        ") {\n" .
        "    \$script->fail_and_die(\"exception %s->new\");\n" .
        "}",
        $new_var_name, $new_var_name, $NEW_MODULE,
        $config_expr, $logger_expr, $db_role_expr, $FLAVOUR, $NEW_MODULE
    );

    my $scope_root = find_scope_root($stmt);
    my $decl_loc   = $stmt->location;

    my $all_symbols = $scope_root->find('PPI::Token::Symbol') || [];
    my @in_scope_refs;
    for my $sym (@$all_symbols) {
        next if $sym->content ne $old_var_name;
        next if loc_cmp($sym->location, $decl_loc) <= 0;
        next if is_within($sym, $stmt);  # the declaration's own token, not a usage
        next if is_shadowed($sym, $bare_old_name, $scope_root, $stmt);
        push @in_scope_refs, $sym;
    }

    # Also catch references inside interpolating strings (e.g. "...$sybase...")
    # and qq//, which PPI does not expose as separate Symbol tokens.
    my $all_strings = $scope_root->find(sub {
        my (undef, $n) = @_;
        return 0 unless $n->isa('PPI::Token::Quote::Double')
                      || $n->isa('PPI::Token::Quote::Interpolate');
        return $n->content =~ /(?<!\\)\$\{?\Q$bare_old_name\E\}?(?!\w)/;
    }) || [];
    my @in_scope_strings;
    for my $tok (@$all_strings) {
        next if loc_cmp($tok->location, $decl_loc) <= 0;
        next if is_shadowed($tok, $bare_old_name, $scope_root, $stmt);
        push @in_scope_strings, $tok;
    }

    (my $new_bare_name = $new_var_name) =~ s/^\$//;

    push @plan, {
        stmt          => $stmt,
        old_name      => $old_var_name,
        new_name      => $new_var_name,
        bare_old_name => $bare_old_name,
        bare_new_name => $new_bare_name,
        symbols       => \@in_scope_refs,
        strings       => \@in_scope_strings,
        new_text      => $new_text,
        role_resolved => $role_resolved,
    };
}

# ---------------------------------------------------------------------
# PHASE B: apply symbol renames first, then renames inside interpolating
# strings that reference the same variable
# ---------------------------------------------------------------------
for my $item (@plan) {
    $_->set_content($item->{new_name}) for @{ $item->{symbols} };

    for my $tok (@{ $item->{strings} }) {
        my $content = $tok->content;
        $content =~ s/(?<!\\)(\$\{?)\Q$item->{bare_old_name}\E(\}?)(?!\w)/$1$item->{bare_new_name}$2/g;
        $tok->set_content($content);
    }
}

# ---------------------------------------------------------------------
# PHASE C: replace each declaration statement with its rebuilt version
# ---------------------------------------------------------------------
for my $item (@plan) {
    my $replacement_doc = PPI::Document->new(\$item->{new_text});
    for my $new_elem ($replacement_doc->children) {  # includes whitespace, to preserve layout
        my $cloned = $new_elem->clone;   # fully detached copy, safe to move across documents
        $item->{stmt}->insert_before($cloned);
    }
    $item->{stmt}->remove;
}

# ---------------------------------------------------------------------
# PHASE D: rewrite the `use Mx::Sybase2;` line(s)
# ---------------------------------------------------------------------
my $includes = $doc->find('PPI::Statement::Include') || [];
for my $inc (@$includes) {
    next unless $inc->module && $inc->module eq $OLD_MODULE;
    my $module_token = $inc->schild(1);
    $module_token->set_content($NEW_MODULE) if $module_token;
}

# ---------------------------------------------------------------------
# PHASE E: write out, report, verify
# ---------------------------------------------------------------------
my $out_file = "$file.converted";
open my $fh, '>', $out_file or die "Can't write $out_file: $!";
print $fh $doc->serialize;
close $fh;

print "Converted -> $out_file\n";
for my $item (@plan) {
    printf "  %s -> %s  (renamed %d reference%s)\n",
        $item->{old_name}, $item->{new_name},
        scalar(@{ $item->{symbols} }), (@{ $item->{symbols} } == 1 ? '' : 's');
    warn "WARNING: could not resolve db_role for $item->{old_name} -- check $item->{new_name}'s db_role value in $out_file\n"
        unless $item->{role_resolved};
}

# Best-effort safety net: flag any occurrence of an old variable name that
# survived the rewrite (e.g. inside a heredoc, regex, or other construct PPI
# does not expose as a Symbol/interpolating-string token), so it gets a
# manual look instead of silently shipping broken/stale code.
my $final_text = join '', map {
    ($_->isa('PPI::Token::Comment') || $_->isa('PPI::Token::Pod')) ? '' : $_->content
} @{ $doc->find('PPI::Token') || [] };
for my $item (@plan) {
    if ($final_text =~ /(?<!\\)\$\{?\Q$item->{bare_old_name}\E\}?(?!\w)/) {
        warn "WARNING: possible unconverted reference to \$$item->{bare_old_name} remains in $out_file -- please review manually.\n";
    }
}

system(qq{perl -I "$FindBin::Bin" -c "$out_file"}) == 0
    or warn "WARNING: $out_file did not pass perl -c, needs manual review\n";
