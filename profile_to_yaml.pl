#!/usr/bin/env perl
#
# profile_to_yaml.pl
#
# Converts a DBI::ProfileDumper file (produced by running your script with
#   DBI_PROFILE=2/DBI::ProfileDumper perl myscript.pl
# which writes a machine-readable "dbi.prof" file) into the YAML report
# format used elsewhere in the harness:
#
#   ---
#   - id: 1
#     sql: SELECT * FROM customers
#     call_count: 3
#     total_time_sec: '0.012400'
#     min_time_sec: '0.003800'
#     max_time_sec: '0.004900'
#     first_called: 'Sun Jul 19 14:02:11 2026'
#     last_called: 'Sun Jul 19 14:02:13 2026'
#
# Usage:
#   perl profile_to_yaml.pl --in dbi.prof --out profile_report.yaml

use strict;
use warnings;
use Getopt::Long;
use DBI::ProfileData;
use YAML::XS qw(DumpFile);

my $in_file  = 'dbi.prof';
my $out_file = 'profile_report.yaml';

GetOptions(
    "in=s"  => \$in_file,
    "out=s" => \$out_file,
) or die "Usage: $0 --in dbi.prof --out profile_report.yaml\n";

die "Error: input file '$in_file' not found\n" unless -e $in_file;

# DBI::ProfileData parses the dumper file and reconstructs a Data structure
# that mirrors $dbh->{Profile}{Data} from a live DBI::Profile object -
# at path level 2, keyed by SQL statement text, each value an arrayref:
#   [count, total_time, first_time, min_time, max_time, first_called_at, last_called_at]
my $profile = DBI::ProfileData->new(File => $in_file);
my $data    = $profile->{Data};

die "Error: no profile data found in '$in_file'\n" unless $data && %$data;

my @report;
my $id = 1;

# Order by first_called timestamp so ID reflects actual execution order
for my $sql (sort { $data->{$a}[5] <=> $data->{$b}[5] } keys %$data) {
    my ($count, $total, $first, $min, $max, $first_called, $last_called)
        = @{ $data->{$sql} };

    push @report, {
        id             => $id++,
        sql            => $sql,
        call_count     => $count,
        total_time_sec => sprintf("%.6f", $total),
        min_time_sec   => sprintf("%.6f", $min),
        max_time_sec   => sprintf("%.6f", $max),
        first_called   => scalar(localtime($first_called)),
        last_called    => scalar(localtime($last_called)),
    };
}

DumpFile($out_file, \@report);

print "Wrote " . scalar(@report) . " entries to $out_file\n";
