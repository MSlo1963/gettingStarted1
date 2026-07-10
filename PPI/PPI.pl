#!/usr/bin/perl
use strict;
use warnings;
use FindBin;
use lib "$FindBin::Bin/..";

use PPI;
use PPI::Transform::RemoveObsoleteUses;
use PPI::Transform::AddUses;
use PPI::Transform::RemoveAccountCreation;
use PPI::Transform::ReplaceSybase2;
use PPI::Transform::ReplaceSybase;
use PPI::Transform::Script;

die "Usage: $0 <file.pm> [<output.pm>]\n" unless @ARGV;
my $input  = shift @ARGV;
my $output = shift @ARGV || "$input.new";

die "No such file: $input\n" unless -f $input;

# 1. Load the Perl file into a PPI Document (The "DOM")
my $doc = PPI::Document->new($input);
die "Failed to parse $input: " . PPI::Document->errstr unless $doc;

# 2. Create the transformation objects
my $remover = PPI::Transform::RemoveObsoleteUses->new;
my $adder   = PPI::Transform::AddUses->new;
my $account_remover = PPI::Transform::RemoveAccountCreation->new;  
my $sybase2_replacer = PPI::Transform::ReplaceSybase2->new;
my $sybase_replacer = PPI::Transform::ReplaceSybase->new; 
my $script_transformer = PPI::Transform::Script->new;

# 3. Apply them one after another in memory
# document() returns the number of changes made (0 on no-op, undef on
# error) -- apply() only returns a success boolean, so it can't be used
# here to total up a real change count.
my $changes = 0;
$changes += $remover->document($doc) || 0;
$changes += $adder->document($doc) || 0;
$changes += $account_remover->document($doc) || 0;

# ReplaceSybase2.pm and ReplaceSybase.pm each return the change count
# plus, for every Mx::Sybase2->new(...)/Mx::Sybase->new(...) statement
# they rewrote, an [old_var, new_var] pair naming the original object's
# variable and the replacement Mx::DB object's variable.
my ($sybase2_changes, @sybase2_var_pairs) = $sybase2_replacer->document($doc);
$changes += $sybase2_changes || 0;
for my $pair (@sybase2_var_pairs) {
    my ($old_var, $new_var) = @$pair;
    print "  Mx::Sybase2 variable renamed: ", ($old_var // '?'), " -> $new_var\n";
}

my ($sybase_changes, @sybase_var_pairs) = $sybase_replacer->document($doc);
$changes += $sybase_changes || 0;
for my $pair (@sybase_var_pairs) {
    my ($old_var, $new_var) = @$pair;
    print "  Mx::Sybase variable renamed: ", ($old_var // '?'), " -> $new_var\n";
}

$changes += $script_transformer->document($doc) || 0;

# 4. Save the document to the output file
if ($changes > 0) {
    if ($doc->save($output)) {
        print "Transformed $input -> $output ($changes changes made)\n";
    } else {
        die "Failed to save to $output: $!";
    }
} else {
    print "No changes were necessary for $input. No file written.\n";
}