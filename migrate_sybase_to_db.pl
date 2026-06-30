#!/usr/bin/env perl
use strict;
use warnings;
use File::Find;
use File::Copy;
use Getopt::Long;

# ---------------------------------------------------------------------------
# migrate_sybase_to_db.pl
#
# Detects and rewrites the pattern:
#
#   unless ($sybase = Mx::Sybase2->new(
#       ...
#       database => 'KBC_MXDBREP',   # or KBC_MXDB
#       ...
#   )) {
#       <failure block>
#   }
#
# Rules:
#   - argument list contains 'KBC_MXDBREP'  =>  db_role => 'REP'
#   - argument list contains 'KBC_MXDB'     =>  db_role => 'FIN'
#     (KBC_MXDBREP is tested first since it contains KBC_MXDB as a substring)
#
# Rewritten to:
#
#   my $db = Mx::DB->new( config => $config, logger => $logger,
#                         auto_account => 1, db_role => 'REP' );
#   unless ($db) {
#       <failure block>
#   }
#
# Also rewrites:
#   use Mx::Sybase2  =>  use Mx::DB
#   use Mx::Sybase   =>  use Mx::DB
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

    my ($rewritten, $matches) = rewrite($original);
    return unless @$matches;

    $total_rewrites += scalar @$matches;
    $changed_files++;

    print "\n", "-" x 60, "\n";
    printf "FILE: %s  (%d rewrite%s)\n", $path, scalar @$matches, @$matches == 1 ? '' : 's';
    print "-" x 60, "\n";

    if ($verbose || $dry_run) {
        for my $m (@$matches) {
            printf "  line %d: %s  =>  %s\n", $m->{line}, $m->{old}, $m->{new};
        }
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
# Core rewriter
# Returns ($new_source, \@matches)
# ---------------------------------------------------------------------------
sub rewrite {
    my ($src) = @_;
    my @matches;

    # Build offset -> line-number lookup
    my @line_starts = (0);
    while ($src =~ /\n/g) { push @line_starts, pos($src) }
    my $offset_to_line = sub {
        my ($off) = @_;
        my ($lo, $hi) = (0, $#line_starts);
        while ($lo < $hi) {
            my $mid = int(($lo + $hi + 1) / 2);
            $line_starts[$mid] <= $off ? ($lo = $mid) : ($hi = $mid - 1);
        }
        return $lo + 1;
    };

    # -----------------------------------------------------------------------
    # Pass 1: find and replace unless($var = Mx::Sybase2?->new(...)) { ... }
    #
    # Walk the source character by character, searching for the keyword
    # "unless" followed by our pattern.  We use index() + manual paren
    # balancing so multiline blocks are handled cleanly without \G.
    # -----------------------------------------------------------------------
    my $new = '';
    my $pos = 0;
    my $len = length($src);

    while (1) {
        # Find next 'unless' from current position
        my $kw_pos = index($src, 'unless', $pos);
        last if $kw_pos == -1;

        # After 'unless' skip optional whitespace then expect '('
        my $after_unless = substr($src, $kw_pos + 6);  # 6 = length('unless')
        unless ($after_unless =~ /\A(\s*)\(/) {
            # 'unless' not followed by '(' — copy up through it and continue
            $new .= substr($src, $pos, $kw_pos - $pos + 6);
            $pos  = $kw_pos + 6;
            next;
        }
        my $ws1      = $1;
        my $cond_open = $kw_pos + 6 + length($ws1) + 1;  # position after '('

        # Check what follows the opening '(' — must be $var = Mx::Sybase2?->new
        my $peek = substr($src, $cond_open);
        unless ($peek =~ /\A\s*\$\w+\s*=\s*Mx::Sybase2?->new\s*\(/) {
            # Not our pattern
            $new .= substr($src, $pos, $kw_pos - $pos + 6 + length($ws1) + 1);
            $pos  = $cond_open;
            next;
        }

        # Balance parens for the condition (we are one level deep after '(')
        my $cond_content = '';
        my $depth = 1;
        my $i = $cond_open;
        while ($i < $len && $depth > 0) {
            my $ch = substr($src, $i, 1);
            $depth++ if $ch eq '(';
            $depth-- if $ch eq ')';
            $cond_content .= $ch unless $depth == 0;
            $i++;
        }
        # $i is now just after the closing ')' of unless(...)

        # Skip optional whitespace then expect '{'
        my $after_cond = substr($src, $i);
        unless ($after_cond =~ /\A(\s*)\{/) {
            # Malformed — pass through
            $new .= substr($src, $pos, $i - $pos);
            $pos  = $i;
            next;
        }
        $i += length($1) + 1;  # skip whitespace + '{'

        # Balance braces for the block body
        my $body = '';
        $depth = 1;
        while ($i < $len && $depth > 0) {
            my $ch = substr($src, $i, 1);
            $depth++ if $ch eq '{';
            $depth-- if $ch eq '}';
            $body .= $ch unless $depth == 0;
            $i++;
        }
        # $i is now just after the closing '}'

        # Determine db_role — KBC_MXDBREP first (it contains KBC_MXDB)
        my $db_role;
        if ($cond_content =~ /KBC_MXDBREP/) {
            $db_role = 'REP';
        } elsif ($cond_content =~ /KBC_MXDB/) {
            $db_role = 'FIN';
        } else {
            warn "WARNING: line "
                . $offset_to_line->($kw_pos)
                . ": Mx::Sybase->new() found but no KBC_MXDB/KBC_MXDBREP in args; skipping.\n";
            # Pass through the entire block unchanged
            $new .= substr($src, $pos, $i - $pos);
            $pos  = $i;
            next;
        }

        # Determine indentation from the text before 'unless' on its line
        my $indent = '';
        my $before_kw = substr($src, $pos, $kw_pos - $pos);
        if ($before_kw =~ /([^\n]*)\z/) {
            ($indent = $1) =~ s/\S.*//s;  # only leading whitespace
        }

        my $replacement =
            "${indent}my \$db = Mx::DB->new( config => \$config, logger => \$logger,\n"
          . "${indent}                       auto_account => 1, db_role => '$db_role' );\n"
          . "${indent}unless (\$db) {"
          . $body
          . "}\n";

        push @matches, {
            line => $offset_to_line->($kw_pos),
            old  => 'Mx::Sybase' . ($cond_content =~ /Mx::Sybase2/ ? '2' : '') . '->new(...) [KBC_MXDB' . ($db_role eq 'REP' ? 'REP' : '') . ']',
            new  => "Mx::DB->new(..., db_role => '$db_role')",
        };

        $new .= substr($src, $pos, $kw_pos - $pos) . $replacement;
        $pos  = $i;
    }

    # Append remainder
    $new .= substr($src, $pos);

    # -----------------------------------------------------------------------
    # Pass 2: rewrite remaining bare Mx::Sybase2?-> references
    # -----------------------------------------------------------------------
    while ($new =~ /\b(Mx::Sybase2?)(->)/g) {
        push @matches, {
            line => $offset_to_line->(pos($new) - length($1) - length($2)),
            old  => $1 . '->',
            new  => 'Mx::DB->',
        };
    }
    $new =~ s/\bMx::Sybase2?->/Mx::DB->/g;

    # -----------------------------------------------------------------------
    # Pass 3: rewrite use Mx::Sybase2 / use Mx::Sybase declarations
    # -----------------------------------------------------------------------
    while ($src =~ /\b(use\s+)(Mx::Sybase2?)\b/g) {
        push @matches, {
            line => $offset_to_line->(pos($src) - length($2)),
            old  => "use $2",
            new  => 'use Mx::DB',
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
