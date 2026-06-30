#!/usr/bin/env perl
use strict;
use warnings;
use File::Find;
use File::Copy;
use Getopt::Long;

# ---------------------------------------------------------------------------
# migrate_sybase_to_db.pl
#
# Detects and rewrites:
#
#   unless ($sybase = Mx::Sybase2->new( ... )) { ... }   (single or multiline)
#   unless ($sybase = Mx::Sybase->new(  ... )) { ... }   (single or multiline)
#
# into:
#
#   unless ($sybase = Mx::DB->new( ... )) { ... }
#
# Also rewrites:
#   use Mx::Sybase2       =>  use Mx::DB
#   use Mx::Sybase        =>  use Mx::DB
#   Mx::Sybase2->method   =>  Mx::DB->method
#   Mx::Sybase->method    =>  Mx::DB->method
#
# Usage:
#   perl migrate_sybase_to_db.pl [options] <file_or_dir> [...]
#
# Options:
#   --dry-run    Show what would change without writing files
#   --backup     Keep a .bak copy of each modified file
#   --verbose    Print each match location and a per-line diff
#   --help       Show this help
# ---------------------------------------------------------------------------

my ($dry_run, $backup, $verbose, $help) = (0, 0, 0, 0);

GetOptions(
    'dry-run'  => \$dry_run,
    'backup'   => \$backup,
    'verbose'  => \$verbose,
    'help'     => \$help,
) or usage();

usage() if $help || !@ARGV;

my ($total_files, $changed_files, $total_rewrites) = (0, 0, 0);

for my $target (@ARGV) {
    if (-f $target) {
        process_file($target);
    } elsif (-d $target) {
        find({
            wanted   => sub { process_file($File::Find::name) if -f $_ && /\.p[lm]$/ },
            no_chdir => 1,
        }, $target);
    } else {
        warn "WARNING: '$target' is not a file or directory, skipping.\n";
    }
}

print "\n", "=" x 60, "\n";
print "Summary\n";
print "=" x 60, "\n";
printf "Files scanned  : %d\n", $total_files;
printf "Files changed  : %d\n", $changed_files;
printf "Total rewrites : %d\n", $total_rewrites;
print "(dry-run: no files were written)\n" if $dry_run;

# ---------------------------------------------------------------------------

sub process_file {
    my ($path) = @_;
    $total_files++;

    open my $fh, '<', $path or do { warn "Cannot read '$path': $!\n"; return };
    my $original = do { local $/; <$fh> };
    close $fh;

    my ($rewritten, $matches) = rewrite($original, $path);
    return unless @$matches;

    $total_rewrites += scalar @$matches;
    $changed_files++;

    print "\n", "-" x 60, "\n";
    printf "FILE: %s  (%d rewrite%s)\n", $path, scalar @$matches, @$matches == 1 ? '' : 's';
    print "-" x 60, "\n";

    if ($verbose || $dry_run) {
        printf "  line %d: %s  =>  Mx::DB\n", $_->{line}, $_->{old} for @$matches;
        print "\n";
        show_diff($original, $rewritten);
    }

    return if $dry_run;

    if ($backup) {
        copy($path, "$path.bak") or warn "Could not create backup '$path.bak': $!\n";
    }

    open my $out, '>', $path or do { warn "Cannot write '$path': $!\n"; return };
    print $out $rewritten;
    close $out;
    print "  -> written\n";
}

# ---------------------------------------------------------------------------
# Core rewriter: returns ($new_source, \@matches)
# Each match: { line => N, old => 'Mx::Sybase2' }
# ---------------------------------------------------------------------------
sub rewrite {
    my ($src) = @_;
    my @matches;

    # Build a line-number index: offset -> line number
    my @line_starts = (0);
    while ($src =~ /\n/g) {
        push @line_starts, pos($src);
    }
    my $offset_to_line = sub {
        my ($off) = @_;
        my $lo = 0; my $hi = $#line_starts;
        while ($lo < $hi) {
            my $mid = int(($lo + $hi + 1) / 2);
            $line_starts[$mid] <= $off ? ($lo = $mid) : ($hi = $mid - 1);
        }
        return $lo + 1;
    };

    # Replace Mx::Sybase2-> and Mx::Sybase-> with Mx::DB->
    # Collect matches first (with correct offsets), then substitute.
    my $new = $src;
    while ($src =~ /\b(Mx::Sybase2?)(->)/g) {
        push @matches, {
            line => $offset_to_line->(pos($src) - length($1) - length($2)),
            old  => $1,
        };
    }
    $new =~ s/\bMx::Sybase2?->/Mx::DB->/g;

    # Replace use Mx::Sybase2 / use Mx::Sybase
    while ($src =~ /\b(use\s+)(Mx::Sybase2?)\b/g) {
        push @matches, {
            line => $offset_to_line->(pos($src) - length($2)),
            old  => "use $2",
        };
    }
    $new =~ s/\buse\s+Mx::Sybase2?\b/use Mx::DB/g;

    return ($new, \@matches);
}

# ---------------------------------------------------------------------------
# Print a compact line-by-line diff (no external tools needed)
# ---------------------------------------------------------------------------
sub show_diff {
    my ($old, $new) = @_;
    my @old_lines = split /\n/, $old, -1;
    my @new_lines = split /\n/, $new, -1;
    my $max = @old_lines > @new_lines ? $#old_lines : $#new_lines;
    for my $i (0 .. $max) {
        my $o = $old_lines[$i];
        my $n = $new_lines[$i];
        next if defined $o && defined $n && $o eq $n;
        printf "  -%4d  %s\n", $i + 1, $o if defined $o;
        printf "  +%4d  %s\n", $i + 1, $n if defined $n;
    }
}

# ---------------------------------------------------------------------------
sub usage {
    print <<'END';
Usage:
  perl migrate_sybase_to_db.pl [options] <file_or_dir> [...]

Options:
  --dry-run    Show what would change, do not write files
  --backup     Save a .bak copy before overwriting each file
  --verbose    Print each match location and a line diff
  --help       Show this message

Examples:
  perl migrate_sybase_to_db.pl --dry-run --verbose lib/
  perl migrate_sybase_to_db.pl --backup src/myscript.pl
  perl migrate_sybase_to_db.pl lib/ scripts/
END
    exit 0;
}
