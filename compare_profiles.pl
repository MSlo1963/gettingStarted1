#!/usr/bin/env perl
#
# compare_profiles.pl
#
# Compares two DBI::ProfileDumper dump files (e.g. one from the legacy
# Sybase run, one from the migrated PostgreSQL run) and reports where
# statement-level execution counts or timings diverge.
#
# Usage:
#   perl compare_profiles.pl legacy_profile.out migrated_profile.out
#
# The profiles should have been captured with the same Path setting on
# both sides, e.g.:
#   $ENV{DBI_PROFILE} = '2/DBI::ProfileDumper';                 # by Statement
#   $ENV{DBI_PROFILE} = '!Statement:!Caller/DBI::ProfileDumper'; # by Statement+Caller
#
# Output is a table sorted by the biggest divergence first (count
# mismatches surface before timing-only mismatches), which is normally
# the fastest way to find the exact query where legacy and migrated
# code paths part ways.

use strict;
use warnings;
use DBI::ProfileData;
use Getopt::Long;

my $top      = 0;      # 0 = show all
my $min_diff = 0;      # only show rows where count differs by at least this
my $csv_out;

GetOptions(
    'top=i'      => \$top,
    'min-diff=i' => \$min_diff,
    'csv=s'      => \$csv_out,
) or die "Usage: $0 [--top N] [--min-diff N] [--csv out.csv] <legacy.dump> <migrated.dump>\n";

my ($legacy_file, $migrated_file) = @ARGV;
die "Usage: $0 [--top N] [--min-diff N] [--csv out.csv] <legacy.dump> <migrated.dump>\n"
    unless $legacy_file && $migrated_file;

my %legacy   = load_profile($legacy_file);
my %migrated = load_profile($migrated_file);

# Union of all statement keys seen on either side
my %all_keys = ( %legacy, %migrated );
my @rows;

for my $key ( keys %all_keys ) {
    my $l = $legacy{$key};
    my $m = $migrated{$key};

    my $l_count = $l ? $l->{count} : 0;
    my $m_count = $m ? $m->{count} : 0;
    my $l_time  = $l ? $l->{total_duration} : 0;
    my $m_time  = $m ? $m->{total_duration} : 0;

    my $count_diff = abs( $l_count - $m_count );
    my $time_diff  = abs( $l_time - $m_time );

    next if $count_diff < $min_diff;

    push @rows, {
        key         => $key,
        legacy_ct   => $l_count,
        migrated_ct => $m_count,
        count_diff  => $count_diff,
        legacy_t    => $l_time,
        migrated_t  => $m_time,
        time_diff   => $time_diff,
        status      => !$l   ? 'ONLY IN MIGRATED'
                     : !$m   ? 'ONLY IN LEGACY'
                     : $count_diff ? 'COUNT MISMATCH'
                     :               'time only',
    };
}

# Sort: count mismatches (incl. missing-on-one-side) first, biggest first;
# then by time diff, biggest first.
@rows = sort {
       ( $b->{count_diff} <=> $a->{count_diff} )
    || ( $b->{time_diff}  <=> $a->{time_diff} )
} @rows;

@rows = @rows[ 0 .. $top - 1 ] if $top && @rows > $top;

print_table(\@rows);
write_csv( $csv_out, \@rows ) if $csv_out;

exit 0;

# ---------------------------------------------------------------------

sub load_profile {
    my ($file) = @_;
    die "Profile file not found: $file\n" unless -f $file;

    my $prof = DBI::ProfileData->new( File => $file );
    my %by_key;

    for my $node ( @{ $prof->nodes } ) {
        my ( $count, $total_duration, $first_duration,
             $shortest, $longest, $first_ts, $last_ts, @path_keys ) = @$node;

        # Join multi-part paths (e.g. Statement + Caller) into one string
        my $key = join( ' || ', map { defined $_ ? $_ : '(undef)' } @path_keys );

        $by_key{$key} = {
            count          => $count,
            total_duration => $total_duration,
            longest        => $longest,
        };
    }

    return %by_key;
}

sub print_table {
    my ($rows) = @_;

    if ( !@$rows ) {
        print "No divergences found (given current --min-diff filter).\n";
        return;
    }

    printf "%-70s %8s %8s %6s %10s %10s %10s\n",
        'STATEMENT (truncated)', 'LEG_CT', 'MIG_CT', 'CTDIF', 'LEG_TIME', 'MIG_TIME', 'STATUS';
    print '-' x 130, "\n";

    for my $r (@$rows) {
        my $label = $r->{key};
        $label =~ s/\s+/ /g;
        $label = substr( $label, 0, 67 ) . '...' if length($label) > 70;

        printf "%-70s %8d %8d %6d %10.4f %10.4f %10s\n",
            $label, $r->{legacy_ct}, $r->{migrated_ct}, $r->{count_diff},
            $r->{legacy_t}, $r->{migrated_t}, $r->{status};
    }
}

sub write_csv {
    my ($file, $rows) = @_;
    open my $fh, '>', $file or die "Cannot write $file: $!\n";
    print $fh join( ',', qw(statement legacy_count migrated_count count_diff
                            legacy_time migrated_time time_diff status) ), "\n";
    for my $r (@$rows) {
        my $stmt = $r->{key};
        $stmt =~ s/"/""/g;
        print $fh qq{"$stmt",$r->{legacy_ct},$r->{migrated_ct},$r->{count_diff},}
                 . qq{$r->{legacy_t},$r->{migrated_t},$r->{time_diff},$r->{status}\n};
    }
    close $fh;
    print "\nCSV written to $file\n";
}
