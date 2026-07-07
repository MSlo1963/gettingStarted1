#!/usr/bin/perl
use strict;
use warnings;
use PPI;

use Data::Dumper;

die "Usage: $0 <file.pl>\n" unless @ARGV;
my $filename = shift @ARGV;
my $doc = PPI::Document->new($filename);

replace_statements($doc, 
    qr/(?=.*Mx::Sybase->new)(?=.*FIN_DB)/s,
    'my $db_fin = Modern::DB->connect( system => "FINANCE", timeout => 30, variant => "sybase" );');

replace_statements($doc,
    qr/(?=.*Mx::Sybase2->new)(?=.*FIN_DB)/s,
    'my $db_fin2 = Modern::DB->connect( system => "FINANCE", timeout => 30, variant => "sybase2");');

# Drop the old `use` lines, but only once nothing in the script still
# refers to that module (e.g. a non-FIN_DB statement we left untouched).
remove_unused_include($doc, 'Mx::Sybase');
remove_unused_include($doc, 'Mx::Sybase2');

# The converted code calls Modern::DB->connect(...), so make sure it's
# actually pulled in -- but don't add it twice if it's already there.
add_include($doc, 'Modern::DB');
add_include($doc, 'Data::Dumper');

# 3. Save
$doc->save("$filename.new");

# Find every statement matching $old_code and swap it for $new_code.
sub replace_statements {
    my ($doc, $old_code, $new_code) = @_;

    # 1. Define the "Find" logic
    # We look for statements that contain the specific legacy pattern
    my $legacy_statements = $doc->find(sub {
        my ($root, $el) = @_;
        return 0 unless $el->isa('PPI::Statement');

        # Match the content of the whole statement against your regex
        return $el->content =~ $old_code;
    });

    # 2. Perform the replacement
    if ($legacy_statements) {
        # The name $new_code declares (e.g. 'my $db_fin = ...' -> '$db_fin').
        my ($new_name) = $new_code =~ /^\s*my\s+(\$\w+)/;

        foreach my $old_stmt (@$legacy_statements) {
            # Rename every other reference to the old variable, throughout
            # the whole document, to the new variable's name.
            my ($orig_name) = $old_stmt->isa('PPI::Statement::Variable')
                ? $old_stmt->variables
                : ();

            if ($orig_name && $new_name && $orig_name ne $new_name) {
                my $symbols = $doc->find(sub {
                    my (undef, $n) = @_;
                    return 0 unless $n->isa('PPI::Token::Symbol');
                    return $n->content eq $orig_name;
                }) || [];
                $_->set_content($new_name) for @$symbols;
            }

            my $new_stmt = PPI::Document->new(\$new_code)->child(0)->clone;

            # Replace the old statement with the new one
            # 'insert_before' + 'delete' is the safest round-trip way to swap
            $old_stmt->insert_before($new_stmt);
            $old_stmt->delete;

            print "Replaced legacy Sybase connection with Modern::DB definition"
                . ($orig_name && $new_name ? " ($orig_name -> $new_name)" : '') . ".\n";
        }
    }

    return $legacy_statements;
}

# Remove a `use $module;` statement, but only if nothing else in the
# document still refers to that module (i.e. every use of it was converted).
sub remove_unused_include {
    my ($doc, $module) = @_;

    my $remaining_refs = $doc->find(sub {
        my (undef, $n) = @_;
        return 0 unless $n->isa('PPI::Token::Word');
        return 0 unless $n->content eq $module;
        return !($n->parent && $n->parent->isa('PPI::Statement::Include'));
    }) || [];
    return if @$remaining_refs;

    my $includes = $doc->find('PPI::Statement::Include') || [];
    for my $inc (@$includes) {
        next unless $inc->module && $inc->module eq $module;

        # Also drop the blank line the include statement sat on.
        my $prev = $inc->previous_sibling;
        $prev->remove if $prev && $prev->isa('PPI::Token::Whitespace') && ($prev->content =~ tr/\n//) <= 1;

        $inc->remove;
        print "Removed unused 'use $module;'.\n";
    }
}

# Add a `use $module;` statement, unless the document already has one.
# Inserted after the last existing `use`/`require`, or at the top if there
# isn't one.
sub add_include {
    my ($doc, $module) = @_;

    my $includes = $doc->find('PPI::Statement::Include') || [];
    return if grep { $_->module && $_->module eq $module } @$includes;

    if (@$includes) {
        # PPI::Statement::insert_after always inserts immediately after the
        # anchor, so repeated inserts on the SAME anchor land in reverse
        # order. Lead with our own "\n" too: the anchor's own line-ending
        # newline may already belong to the *following* sibling's whitespace,
        # so we can't rely on one being there already.
        my $mini = PPI::Document->new(\"\nuse $module;");
        my $anchor = $includes->[-1];
        $anchor->insert_after($_->clone) for reverse $mini->children;
    }
    else {
        # PPI::Statement::insert_before on a fixed anchor preserves order.
        my $mini = PPI::Document->new(\"use $module;\n");
        my ($first_stmt) = @{ $doc->find('PPI::Statement') || [] };
        if ($first_stmt) {
            $first_stmt->insert_before($_->clone) for $mini->children;
        }
    }

    print "Added 'use $module;'.\n";
}
