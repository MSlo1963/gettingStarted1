#!/usr/bin/perl
##############################################################################
# compare_tables.pl
#
# Standalone table comparator for Sybase vs PostgreSQL migration validation.
# Use this AFTER you've already run the legacy script against Sybase and the
# migrated script against Postgres independently. This script does not run
# anything against either script — it only compares resulting table state.
#
# Strategy:
#   1. Compute one aggregate hash per table per DB. If they match, done —
#      no need to pull/compare individual rows.
#   2. If they don't match, pull per-row hashes from both sides and report:
#        - how many rows match
#        - how many rows differ (present both sides, different values)
#        - how many rows exist only on one side
#      then print up to --sample rows of each mismatch category so you can
#      see the actual data, not just counts.
#
# USAGE:
#   ./compare_tables.pl --config db_config.pl --tables tables_config.pl
#   ./compare_tables.pl --config db_config.pl --tables tables_config.pl --only=orders
#   ./compare_tables.pl --config db_config.pl --tables tables_config.pl --sample=50
#
# See __END__ for config file formats.
##############################################################################

use strict;
use warnings;
use DBI;
use Digest::MD5 qw(md5_hex);
use Getopt::Long;

# ---------------------------------------------------------------------------
# CLI options
# ---------------------------------------------------------------------------
my $db_config_file    = 'db_config.pl';
my $tables_config_file = 'tables_config.pl';
my $only;
my $sample = 20;
my $report_csv;

GetOptions(
    'config=s'  => \$db_config_file,
    'tables=s'  => \$tables_config_file,
    'only=s'    => \$only,
    'sample=i'  => \$sample,
    'report=s'  => \$report_csv,
) or die "Bad options\n";

die "DB config file '$db_config_file' not found\n" unless -f $db_config_file;
die "Tables config file '$tables_config_file' not found\n" unless -f $tables_config_file;

my $DB = do $db_config_file
    or die "Failed to load '$db_config_file': " . ($@ || $!) . "\n";
my $TABLES = do $tables_config_file
    or die "Failed to load '$tables_config_file': " . ($@ || $!) . "\n";

my @table_specs = @$TABLES;
@table_specs = grep { $_->{name} eq $only } @table_specs if $only;
die "No tables matched\n" unless @table_specs;

my $csv_fh;
if ($report_csv) {
    open($csv_fh, '>', $report_csv) or die "Can't open $report_csv: $!\n";
    print $csv_fh "table,status,sybase_rows,postgres_rows,matched,different,only_sybase,only_postgres\n";
}

# ---------------------------------------------------------------------------
# Connect once, reuse for all tables
# ---------------------------------------------------------------------------
my $sybase_dbh = connect_db($DB->{sybase});
my $pg_dbh     = connect_db($DB->{postgres});

my $overall_ok = 1;

for my $spec (@table_specs) {
    $overall_ok &&= compare_table($spec);
}

$sybase_dbh->disconnect;
$pg_dbh->disconnect;
close $csv_fh if $csv_fh;

print "\n" . ($overall_ok ? "ALL TABLES MATCH" : "MISMATCHES FOUND — see above") . "\n";
exit($overall_ok ? 0 : 1);

# ---------------------------------------------------------------------------
# compare_table($spec) -> 1 if match, 0 if mismatch
# ---------------------------------------------------------------------------
sub compare_table {
    my ($spec) = @_;
    my $table = $spec->{name};

    print "\n=== $table ===\n";

    # Step 1: cheap whole-table check
    my $sybase_summary = table_hash($sybase_dbh, $spec);
    my $pg_summary      = table_hash($pg_dbh, $spec);

    if ($sybase_summary->{table_hash} eq $pg_summary->{table_hash}) {
        print "MATCH  (sybase=$sybase_summary->{row_count} rows, postgres=$pg_summary->{row_count} rows)\n";
        print $csv_fh "$table,MATCH,$sybase_summary->{row_count},$pg_summary->{row_count},$sybase_summary->{row_count},0,0,0\n"
            if $csv_fh;
        return 1;
    }

    # Step 2: fall back to row-level detail
    print "MISMATCH  (sybase=$sybase_summary->{row_count} rows, postgres=$pg_summary->{row_count} rows)\n";

    my %sy_hashes = %{ $sybase_summary->{row_hashes} };
    my %pg_hashes = %{ $pg_summary->{row_hashes} };

    my @common      = grep { exists $pg_hashes{$_} } keys %sy_hashes;
    my @matched     = grep { $sy_hashes{$_} eq $pg_hashes{$_} } @common;
    my @different   = grep { $sy_hashes{$_} ne $pg_hashes{$_} } @common;
    my @only_sybase = grep { !exists $pg_hashes{$_} } keys %sy_hashes;
    my @only_pg     = grep { !exists $sy_hashes{$_} } keys %pg_hashes;

    printf "  matched:       %d rows\n", scalar(@matched);
    printf "  different:     %d rows\n", scalar(@different);
    printf "  only sybase:   %d rows\n", scalar(@only_sybase);
    printf "  only postgres: %d rows\n", scalar(@only_pg);

    print $csv_fh join(',', $table, 'MISMATCH',
        $sybase_summary->{row_count}, $pg_summary->{row_count},
        scalar(@matched), scalar(@different),
        scalar(@only_sybase), scalar(@only_pg)) . "\n" if $csv_fh;

    if (@different) {
        print "\n  -- sample of $sample differing rows (pk | sybase values | postgres values) --\n";
        for my $pk ((sort @different)[0 .. min($sample - 1, $#different)]) {
            print "  pk=$pk\n";
            print "    sybase:   " . join(', ', map { $_ // 'NULL' } @{ $sybase_summary->{rows}{$pk} }) . "\n";
            print "    postgres: " . join(', ', map { $_ // 'NULL' } @{ $pg_summary->{rows}{$pk} }) . "\n";
        }
    }

    if (@only_sybase) {
        print "\n  -- sample of " . min($sample, scalar @only_sybase) . " rows only in sybase --\n";
        for my $pk ((sort @only_sybase)[0 .. min($sample - 1, $#only_sybase)]) {
            print "  pk=$pk  " . join(', ', map { $_ // 'NULL' } @{ $sybase_summary->{rows}{$pk} }) . "\n";
        }
    }

    if (@only_pg) {
        print "\n  -- sample of " . min($sample, scalar @only_pg) . " rows only in postgres --\n";
        for my $pk ((sort @only_pg)[0 .. min($sample - 1, $#only_pg)]) {
            print "  pk=$pk  " . join(', ', map { $_ // 'NULL' } @{ $pg_summary->{rows}{$pk} }) . "\n";
        }
    }

    return 0;
}

# ---------------------------------------------------------------------------
# table_hash($dbh, $spec) -> {
#   table_hash => md5 of all row hashes,
#   row_count  => n,
#   row_hashes => { pk_str => row_hash },
#   rows       => { pk_str => \@normalized_values },  # kept for diff reporting
# }
# ---------------------------------------------------------------------------
sub table_hash {
    my ($dbh, $spec) = @_;
    my $table = $spec->{name};
    my @pk    = @{ $spec->{pk} };
    my @cols  = @{ $spec->{columns} };
    my %prec  = %{ $spec->{float_precision} || {} };

    my $col_list = join(', ', @pk, @cols);
    my $order_by = join(', ', @pk);
    my $sth = $dbh->prepare("SELECT $col_list FROM $table ORDER BY $order_by");
    $sth->execute;

    my (%row_hashes, %rows_normalized);
    while (my @row = $sth->fetchrow_array) {
        my @pk_vals  = @row[0 .. $#pk];
        my @col_vals = @row[scalar(@pk) .. $#row];

        my @normalized = map { normalize_value($col_vals[$_], $cols[$_], \%prec) } (0 .. $#cols);

        my $pk_str  = join('|', map { defined $_ ? $_ : 'NULL' } @pk_vals);
        my $row_str = join('|', map { defined $_ ? $_ : 'NULL' } @normalized);

        $row_hashes{$pk_str}      = md5_hex($row_str);
        $rows_normalized{$pk_str} = \@normalized;
    }

    my $table_hash = md5_hex(join(',', map { "$_=$row_hashes{$_}" } sort keys %row_hashes));

    return {
        table_hash => $table_hash,
        row_count  => scalar(keys %row_hashes),
        row_hashes => \%row_hashes,
        rows       => \%rows_normalized,
    };
}

# ---------------------------------------------------------------------------
sub normalize_value {
    my ($val, $col, $prec) = @_;
    return undef unless defined $val;

    if ($val =~ /^-?\d+\.?\d*$/) {
        if (exists $prec->{$col}) {
            return sprintf("%.*f", $prec->{$col}, $val);
        }
        $val += 0;
        return "$val";
    }

    if ($val =~ /^\d{4}[-\/]\d{2}[-\/]\d{2}([ T]\d{2}:\d{2}:\d{2})?/) {
        (my $norm = $val) =~ s{/}{-}g;
        $norm =~ s/T/ /;
        $norm =~ s/\s+$//;
        return $norm;
    }

    $val =~ s/^\s+|\s+$//g;
    return $val;
}

sub connect_db {
    my ($conn) = @_;
    return DBI->connect($conn->{dsn}, $conn->{user}, $conn->{pass}, {
        RaiseError => 1,
        AutoCommit => 1,
    });
}

sub min { my $m = shift; for (@_) { $m = $_ if $_ < $m } return $m; }

__END__

=head1 CONFIG FILES

Two separate config files, kept separate from the harness's test_cases.pl
so this script can be run standalone, any time, without needing to know
about legacy/migrated commands.

=head2 db_config.pl

    {
        sybase => {
            dsn  => 'dbi:Sybase:server=SYBSRV;database=mydb',
            user => 'sybuser',
            pass => 'sybpass',
        },
        postgres => {
            dsn  => 'dbi:Pg:dbname=mydb;host=localhost',
            user => 'pguser',
            pass => 'pgpass',
        },
    }

=head2 tables_config.pl

    [
        {
            name            => 'orders',
            pk              => ['order_id'],
            columns         => ['customer_id', 'total_amount', 'status', 'updated_at'],
            float_precision => { total_amount => 2 },
        },
        {
            name            => 'order_lines',
            pk              => ['order_id', 'line_no'],
            columns         => ['sku', 'qty', 'unit_price'],
            float_precision => { unit_price => 4 },
        },
    ]

=head1 OPTIONS

  --config=FILE   db_config.pl path (default: db_config.pl)
  --tables=FILE   tables_config.pl path (default: tables_config.pl)
  --only=NAME     compare only this one table
  --sample=N      how many example rows to print per mismatch category (default 20)
  --report=FILE   optional CSV summary output (table,status,counts...)

=head1 EXIT CODE

  0  all tables matched
  1  at least one table mismatched

Useful for wiring into a CI step or a wrapper script:

    ./compare_tables.pl --config db_config.pl --tables tables_config.pl \
        && echo "safe to proceed" || echo "investigate mismatches"

=cut
