#!/usr/bin/perl
use strict;
use warnings;
use FindBin;
use PPI;

# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------
# Real scripts pass database => $config->SOME_ACCESSOR rather than a literal
# string. Map the accessor name to the db_role value Mx::DB expects.
my %accessor_role_map = (
    DB_NAME => 'FIN',
    DB_REP  => 'REP',
);

my $OLD_MODULE    = 'Mx::Sybase2';
my $NEW_MODULE    = 'Mx::DB';
my $ACCOUNT_MODULE = 'Mx::Account';   # no longer needed once auto_account => 1 is used
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

    # The only valid outcomes are db_role => 'FIN'/'REP' and $fin_db/$rep_db --
    # if we can't confidently resolve to one of those, leave this declaration
    # untouched rather than guess a different name or role.
    my $db_arg = $args{database} // '';
    my ($db_role, $new_var_name);
    if ($db_arg =~ /^\$\w+\s*->\s*(\w+)$/) {
        my $accessor = $1;
        $db_role = $accessor_role_map{$accessor};
    }
    if ($db_role && $db_role eq 'FIN') {
        $new_var_name = '$fin_db';
    }
    elsif ($db_role && $db_role eq 'REP') {
        $new_var_name = '$rep_db';
    }
    else {
        warn "WARNING: could not resolve $old_var_name to a FIN/REP db_role"
           . " (database => $db_arg) -- left unconverted, please handle manually.\n";
        next;
    }
    my $db_role_expr = "'$db_role'";

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
# PHASE D: wrap each renamed variable's `->open();` call in the same
# unless/fail_and_die failure handling used for ->new().
# ---------------------------------------------------------------------
for my $item (@plan) {
    my $open_stmts = $doc->find(sub {
        my (undef, $n) = @_;
        return 0 unless $n->isa('PPI::Statement');
        return $n->content =~ /^\Q$item->{new_name}\E\s*->\s*open\s*\(\s*\)\s*;?\s*$/;
    }) || [];

    for my $stmt (@$open_stmts) {
        my $new_text = sprintf(
            "unless (%s->open()) {\n" .
            "    \$script->fail_and_die('exception on %s->open');\n" .
            "}",
            $item->{new_name}, $item->{new_name}
        );
        my $replacement_doc = PPI::Document->new(\$new_text);
        for my $new_elem ($replacement_doc->children) {
            my $cloned = $new_elem->clone;
            $stmt->insert_before($cloned);
        }
        $stmt->remove;
    }
}

# ---------------------------------------------------------------------
# PHASE E: fix up `use` line(s). If every Mx::Sybase2->new(...) call in the
# file got converted, just rename `use Mx::Sybase2;` to `use Mx::DB;`. If
# some were left unconverted (db_role couldn't be resolved), Mx::Sybase2 is
# still needed at runtime, so keep it and add `use Mx::DB;` alongside it.
# ---------------------------------------------------------------------
if (@plan) {
    my $remaining_old_refs = $doc->find(sub {
        my (undef, $n) = @_;
        return 0 unless $n->isa('PPI::Token::Word');
        return 0 unless $n->content eq $OLD_MODULE;
        return !($n->parent && $n->parent->isa('PPI::Statement::Include'));
    }) || [];

    my $includes = $doc->find('PPI::Statement::Include') || [];
    my ($old_include) = grep { $_->module && $_->module eq $OLD_MODULE } @$includes;
    my ($new_include) = grep { $_->module && $_->module eq $NEW_MODULE } @$includes;

    if ($old_include) {
        if (@$remaining_old_refs) {
            unless ($new_include) {
                my $mini = PPI::Document->new(\"\nuse $NEW_MODULE;");
                $old_include->insert_after($_->clone) for reverse $mini->children;
            }
        }
        else {
            my $module_token = $old_include->schild(1);
            $module_token->set_content($NEW_MODULE) if $module_token;
        }
    }
}

# ---------------------------------------------------------------------
# PHASE F: remove `my $x = Mx::Account->new(...);` declarations entirely.
# With auto_account => 1, Mx::DB resolves the account itself, so the
# explicit Mx::Account object -- and any comment lines sitting directly
# above it explaining it -- is no longer needed.
# ---------------------------------------------------------------------
my $account_decls = $doc->find(sub {
    my (undef, $n) = @_;
    return 0 unless $n->isa('PPI::Statement::Variable');
    return 0 unless $n->type eq 'my';
    return $n->content =~ /\Q$ACCOUNT_MODULE\E\s*->\s*new\b/;
}) || [];

for my $stmt (@$account_decls) {
    my @to_remove;
    my $prev = $stmt->previous_sibling;
    while ($prev) {
        if ($prev->isa('PPI::Token::Comment')) {
            push @to_remove, $prev;
        }
        elsif ($prev->isa('PPI::Token::Whitespace') && ($prev->content =~ tr/\n//) <= 1) {
            push @to_remove, $prev;   # indentation, or the single newline before the comment block
        }
        else {
            last;
        }
        $prev = $prev->previous_sibling;
    }
    $_->remove for @to_remove;
    $stmt->remove;
}

# Drop `use Mx::Account;` too, but only if nothing still references it
# (e.g. a non-`my` assignment we didn't touch).
if (@$account_decls) {
    my $remaining_account_refs = $doc->find(sub {
        my (undef, $n) = @_;
        return 0 unless $n->isa('PPI::Token::Word');
        return 0 unless $n->content eq $ACCOUNT_MODULE;
        return !($n->parent && $n->parent->isa('PPI::Statement::Include'));
    }) || [];

    unless (@$remaining_account_refs) {
        my $acct_includes = $doc->find('PPI::Statement::Include') || [];
        for my $inc (@$acct_includes) {
            next unless $inc->module && $inc->module eq $ACCOUNT_MODULE;
            my $prev = $inc->previous_sibling;
            $prev->remove if $prev && $prev->isa('PPI::Token::Whitespace') && ($prev->content =~ tr/\n//) <= 1;
            $inc->remove;
        }
    }
}

# ---------------------------------------------------------------------
# PHASE G: write out, report, verify
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
