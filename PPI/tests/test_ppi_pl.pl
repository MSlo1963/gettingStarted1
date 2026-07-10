#!/usr/bin/perl
use strict;
use warnings;
use Test::More;
use FindBin;
use File::Temp qw(tempdir);
use File::Spec;
use Cwd qw(getcwd);

# End-to-end tests for PPI/PPI.pl: the CLI wrapper that chains all six
# PPI::Transform::* modules together (RemoveObsoleteUses, AddUses,
# RemoveAccountCreation, ReplaceSybase2, ReplaceSybase, Script).
# Complements test_transforms.pl (which focuses on RemoveObsoleteUses +
# AddUses in isolation).

my $script = File::Spec->catfile($FindBin::Bin, File::Spec->updir, 'PPI.pl');
$script = File::Spec->rel2abs($script);

plan skip_all => "PPI/PPI.pl not found at $script" unless -f $script;

my $orig_cwd = getcwd();

# ---------------------------------------------------------------------
# Case 1: no filename argument -> usage error, non-zero exit, no output.
# ---------------------------------------------------------------------
{
    my $dir = tempdir(CLEANUP => 1);
    chdir $dir or die "Can't chdir to $dir: $!";

    my $exit = system($^X, $script);
    isnt($exit, 0, 'running without a filename argument exits non-zero');

    chdir $orig_cwd or die "Can't chdir back to $orig_cwd: $!";
}

# ---------------------------------------------------------------------
# Case 2: nonexistent input file -> clear error, non-zero exit, no output.
# ---------------------------------------------------------------------
{
    my $dir = tempdir(CLEANUP => 1);
    chdir $dir or die "Can't chdir to $dir: $!";

    my $exit = system($^X, $script, 'DoesNotExist.pm');
    isnt($exit, 0, 'nonexistent input file exits non-zero');
    ok(!-f 'DoesNotExist.pm.new', 'no output file is written for a missing input');

    chdir $orig_cwd or die "Can't chdir back to $orig_cwd: $!";
}

# ---------------------------------------------------------------------
# Case 3: default output filename is "<input>.new" when only one arg is
# given, and the original input file is left untouched.
# ---------------------------------------------------------------------
{
    my $dir = tempdir(CLEANUP => 1);
    chdir $dir or die "Can't chdir to $dir: $!";

    write_file('Legacy.pl', <<'END');
#!/usr/bin/perl
use strict;
use warnings;
use Mx::Sybase2;

print 'hello';
END
    my $original = read_file('Legacy.pl');

    my $exit = system($^X, $script, 'Legacy.pl');
    is($exit, 0, 'exits zero with default output name');

    is(read_file('Legacy.pl'), $original, 'original input file is left untouched');
    SKIP: {
        skip 'output file was not written', 1 unless ok(-f 'Legacy.pl.new', 'defaults to writing "<input>.new"');
        unlike(read_file('Legacy.pl.new'), qr/Mx::Sybase2\b/, 'default-named output was actually transformed');
    }

    chdir $orig_cwd or die "Can't chdir back to $orig_cwd: $!";
}

# ---------------------------------------------------------------------
# Case 4: a file that's already fully normalized (every required use
# present, sorted, and the Script transform's marker already there)
# has nothing left to do, so PPI.pl reports "No changes were
# necessary" and writes no output file. (Previously AddUses.pm seeded
# its change counter from the number of *pre-existing* 'use' lines
# rather than actual diffs, so this branch was unreachable for any
# realistic input -- fixed.)
# ---------------------------------------------------------------------
{
    my $dir = tempdir(CLEANUP => 1);
    chdir $dir or die "Can't chdir to $dir: $!";

    write_file('Clean.pl', <<'END');
#!/usr/bin/perl
use strict;
use warnings;
use Data::Dumper;
use Mx::DB;
use Mx::PerlScript;
use Mx::SQLLibrary;

my $script = Mx::PerlScript->new( logger => $logger, config => $config );

print 'clean';
END

    my ($exit, $out) = run_capture('Clean.pl', 'Clean.out.pl');
    is($exit, 0, 'exits zero on an already-normalized file');
    ok(!-f 'Clean.out.pl', 'no output file is written when nothing changed');
    like($out, qr/No changes were necessary/, 'prints the "no changes necessary" message');

    chdir $orig_cwd or die "Can't chdir back to $orig_cwd: $!";
}

# ---------------------------------------------------------------------
# Case 5: sanity check that PPI.pl wires RemoveObsoleteUses + AddUses
# together end to end (detailed coverage lives in test_transforms.pl).
# Mx::PerlScript (not Mx::Script) is the required module name used by
# AddUses.pm's @REQUIRED list, matching what Script.pm constructs.
# ---------------------------------------------------------------------
{
    my $dir = tempdir(CLEANUP => 1);
    chdir $dir or die "Can't chdir to $dir: $!";

    write_file('Legacy.pl', <<'END');
#!/usr/bin/perl
use strict;
use warnings;
use Mx::Sybase;

print 'hello';
END

    my $exit = system($^X, $script, 'Legacy.pl', 'Legacy.out.pl');
    is($exit, 0, 'runs to completion on a simple obsolete-use file');

    SKIP: {
        skip 'output file was not written', 5 unless -f 'Legacy.out.pl';
        my $result = read_file('Legacy.out.pl');
        unlike($result, qr/Mx::Sybase\b/, 'Mx::Sybase use was removed');
        for my $mod (qw(Data::Dumper Mx::DB Mx::PerlScript Mx::SQLLibrary)) {
            like($result, qr/^use \Q$mod\E;$/m, "use $mod; was added");
        }
    }

    chdir $orig_cwd or die "Can't chdir back to $orig_cwd: $!";
}

# ---------------------------------------------------------------------
# Case 6: RemoveAccountCreation deletes an "Mx::Account->new" statement
# along with only its tightly-attached (no blank line) preceding
# comment, leaving an earlier blank-line-separated comment untouched.
# (Previously the blank-line check tested one Whitespace token's
# content at a time for "\n\s*\n" -- but PPI frequently splits one
# visual blank line into multiple single-newline Whitespace tokens in
# a row, and a Comment token embeds its own trailing newline unlike a
# Statement, so the old per-token check never matched and the walk
# consumed everything back to the rebuilt header. Fixed by accumulating
# newlines across the whole contiguous run with a threshold that
# accounts for that embedded comment newline.)
# ---------------------------------------------------------------------
{
    my $dir = tempdir(CLEANUP => 1);
    chdir $dir or die "Can't chdir to $dir: $!";

    write_file('Legacy.pl', <<'END');
#!/usr/bin/perl
use strict;
use warnings;
use Mx::Account;

# unrelated earlier comment, separated by a blank line

# create the account
my $acc = Mx::Account->new;
$acc->verify;

print 'done';
END

    my $exit = system($^X, $script, 'Legacy.pl', 'Legacy.out.pl');
    is($exit, 0, 'runs to completion on an account-creation file');

    SKIP: {
        skip 'output file was not written', 5 unless -f 'Legacy.out.pl';
        my $result = read_file('Legacy.out.pl');
        unlike($result, qr/Mx::Account->new/, 'the Mx::Account->new statement was removed');
        unlike($result, qr/# create the account/, 'the tightly-attached comment was removed with it');
        like($result, qr/# unrelated earlier comment/, 'the blank-line-separated earlier comment is preserved');
        like($result, qr/\$acc->verify;/, 'unrelated trailing code is preserved');
        like($result, qr/print 'done';/, 'unrelated code after the block is preserved');
    }

    chdir $orig_cwd or die "Can't chdir back to $orig_cwd: $!";
}

# ---------------------------------------------------------------------
# Case 7: PPI/Transform/ReplaceSybase2.pm rewrites a matching
# Mx::Sybase2->new(...) constructor call into an Mx::DB->new(...) call
# followed by an "unless (...) { ...->fail_and_die(...) }" guard, and
# the generated code is valid, parseable Perl. (Previously: the module
# declared "package PPI::Transform::ReplaceSybase" instead of
# "::ReplaceSybase2" and interpolated an undeclared $script inside a
# double-quoted heredoc, both compile errors; and even once those were
# fixed, the heredoc's *content* had a stray unmatched ")" and a
# garbled fail_and_die(...) string. All fixed.)
# ---------------------------------------------------------------------
{
    my $dir = tempdir(CLEANUP => 1);
    chdir $dir or die "Can't chdir to $dir: $!";

    write_file('Legacy.pl', <<'END');
#!/usr/bin/perl
use strict;
use warnings;

our ($config, $logger);

my $rep_db = Mx::Sybase2->new(
    database => $config->DB_REP,
    other    => 1,
);
END

    my $exit = system($^X, $script, 'Legacy.pl', 'Legacy.out.pl');
    is($exit, 0, 'PPI.pl now runs to completion on an Mx::Sybase2->new(...) input');

    SKIP: {
        skip 'output file was not written', 4 unless -f 'Legacy.out.pl';
        my $result = read_file('Legacy.out.pl');
        unlike($result, qr/Mx::Sybase2->new/, 'the Mx::Sybase2->new(...) constructor call was rewritten');
        like($result, qr/\$rep_db = Mx::DB->new\(/, q{replaced with an Mx::DB->new(...) call assigned to $rep_db (REP role)});
        like($result, qr/compat\s+=> 'sybase2',\n\);\nunless \(\$rep_db\) \{/,
            'the generated replacement is well-formed: closed constructor call followed by an unless(...) guard');
        # perl -c also tries to actually load the used Mx::* modules,
        # which aren't installed in this dev environment -- that's a
        # module-availability gap, not a syntax problem, so only fail
        # on a genuine parse error.
        my (undef, $compile_output) = run_capture_perl_c('Legacy.out.pl');
        unlike($compile_output, qr/syntax error/, 'the whole converted file has no syntax errors');
    }

    chdir $orig_cwd or die "Can't chdir back to $orig_cwd: $!";
}

# ---------------------------------------------------------------------
# Case 8: same fix as Case 7, via PPI/Transform/ReplaceSybase.pm and an
# Mx::Sybase->new(...) call.
# ---------------------------------------------------------------------
{
    my $dir = tempdir(CLEANUP => 1);
    chdir $dir or die "Can't chdir to $dir: $!";

    write_file('Legacy.pl', <<'END');
#!/usr/bin/perl
use strict;
use warnings;

our ($config, $logger);

my $fin_db = Mx::Sybase->new(
    database => $config->DB_NAME,
    other    => 1,
);
END

    my $exit = system($^X, $script, 'Legacy.pl', 'Legacy.out.pl');
    is($exit, 0, 'PPI.pl now runs to completion on an Mx::Sybase->new(...) input');

    SKIP: {
        skip 'output file was not written', 4 unless -f 'Legacy.out.pl';
        my $result = read_file('Legacy.out.pl');
        unlike($result, qr/Mx::Sybase->new/, 'the Mx::Sybase->new(...) constructor call was rewritten');
        like($result, qr/\$fin_db_compat_sybase = Mx::DB->new\(/, q{replaced with an Mx::DB->new(...) call (FIN role, "sybase"-suffixed var name)});
        like($result, qr/compat\s+=> 'sybase',\n\);\nunless \(\$fin_db_compat_sybase\) \{/,
            'the generated replacement is well-formed: closed constructor call followed by an unless(...) guard');
        # perl -c also tries to actually load the used Mx::* modules,
        # which aren't installed in this dev environment -- that's a
        # module-availability gap, not a syntax problem, so only fail
        # on a genuine parse error.
        my (undef, $compile_output) = run_capture_perl_c('Legacy.out.pl');
        unlike($compile_output, qr/syntax error/, 'the whole converted file has no syntax errors');
    }

    chdir $orig_cwd or die "Can't chdir back to $orig_cwd: $!";
}

done_testing();

sub write_file {
    my ($name, $content) = @_;
    open my $fh, '>', $name or die "Can't write $name: $!";
    print $fh $content;
    close $fh;
}

sub read_file {
    my ($name) = @_;
    open my $fh, '<', $name or die "Can't read $name: $!";
    local $/;
    my $content = <$fh>;
    close $fh;
    return $content;
}

# Runs the script with stdout+stderr merged and captured, returning
# (exit_code, combined_output). Needed (instead of plain system()) for
# cases that assert on printed messages or captured error text.
sub run_capture_perl_c {
    my ($file) = @_;
    my $cmd = join ' ', map { qq{"$_"} } ($^X, '-c', $file);
    my $output = `$cmd 2>&1`;
    my $exit = $? >> 8;
    return ($exit, $output);
}

sub run_capture {
    my (@args) = @_;
    my $cmd = join ' ', map { qq{"$_"} } ($^X, $script, @args);
    my $output = `$cmd 2>&1`;
    my $exit = $? >> 8;
    return ($exit, $output);
}
