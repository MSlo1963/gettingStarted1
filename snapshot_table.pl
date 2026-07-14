#!/usr/bin/perl
##############################################################################
# snapshot_table.pl
#
# Takes a snapshot of a single table (any engine reachable via DBI) to a
# flat file, and restores a table from such a snapshot. Useful for quickly
# resetting one table to a known state between test runs, without needing
# a full baseline restore across every table.
#
# Works against either Sybase or Postgres — the engine is just whichever
# connection you pass in from db_config.pl. Snapshot format is JSON-lines
# (one row per line), which avoids all the usual delimiter/NULL/escaping
# headaches you get with CSV or bcp-style flat files.
#
# USAGE:
#   Snapshot:
#     ./snapshot_table.pl snapshot --config db_config.pl --engine sybase \
#         --table orders --out snapshots/orders_sybase.snap
#
#   Restore:
#     ./snapshot_table.pl restore --config db_config.pl --engine postgres \
#         --table orders --in snapshots/orders_postgres.snap
#
#   Restore with identity/serial resync (recommended if the table has an
#   auto-generated PK and the script under test will insert new rows):
#     ./snapshot_table.pl restore --config db_config.pl --engine postgres \
#         --table orders --in snapshots/orders_postgres.snap --identity-col order_id
#
# See __END__ for db_config.pl format and more notes.
##############################################################################

use strict;
use warnings;
use DBI;
use JSON::PP qw(encode_json decode_json);
use Getopt::Long;

my $mode = shift @ARGV or die usage();
die usage() unless $mode eq 'snapshot' || $mode eq 'restore';

my $config_file   = 'db_config.pl';
my $engine;
my $table;
my $out_file;
my $in_file;
my $identity_col;
my $batch_size = 500;

GetOptions(
    'config=s'       => \$config_file,
    'engine=s'       => \$engine,
    'table=s'        => \$table,
    'out=s'          => \$out_file,
    'in=s'           => \$in_file,
    'identity-col=s' => \$identity_col,
    'batch-size=i'   => \$batch_size,
) or die usage();

die "Missing --engine (sybase|postgres)\n" unless $engine;
die "Missing --table\n" unless $table;
die "DB config file '$config_file' not found\n" unless -f $config_file;

my $DB = do $config_file
    or die "Failed to load '$config_file': " . ($@ || $!) . "\n";
my $conn = $DB->{$engine}
    or die "No connection config for engine '$engine' in $config_file\n";

my $dbh = DBI->connect($conn->{dsn}, $conn->{user}, $conn->{pass}, {
    RaiseError => 1,
    AutoCommit => 1,
});

if ($mode eq 'snapshot') {
    die "Missing --out\n" unless $out_file;
    do_snapshot($dbh, $table, $out_file);
} else {
    die "Missing --in\n" unless $in_file;
    do_restore($dbh, $table, $in_file, $identity_col, $engine, $batch_size);
}

$dbh->disconnect;

# ---------------------------------------------------------------------------
# do_snapshot($dbh, $table, $out_file)
#   Writes one JSON line per row, plus a header line with column names and
#   a row count, so restore can validate it read the file completely.
# ---------------------------------------------------------------------------
sub do_snapshot {
    my ($dbh, $table, $out_file) = @_;

    my $sth = $dbh->prepare("SELECT * FROM $table");
    $sth->execute;
    my @cols = @{ $sth->{NAME} };

    open(my $fh, '>', $out_file) or die "Can't open $out_file for writing: $!\n";
    print $fh encode_json({ _header => 1, table => $table, columns => \@cols }) . "\n";

    my $count = 0;
    while (my $row = $sth->fetchrow_arrayref) {
        print $fh encode_json([@$row]) . "\n";
        $count++;
    }
    print $fh encode_json({ _footer => 1, row_count => $count }) . "\n";
    close $fh;

    print "Snapshot written: $out_file ($count rows, " . scalar(@cols) . " columns)\n";
}

# ---------------------------------------------------------------------------
# do_restore($dbh, $table, $in_file, $identity_col, $engine, $batch_size)
#   Truncates the table, reloads every row from the snapshot, verifies the
#   row count against the snapshot's footer, and optionally resyncs the
#   identity/serial column so subsequent auto-generated IDs don't diverge
#   from what the other engine will produce.
# ---------------------------------------------------------------------------
sub do_restore {
    my ($dbh, $table, $in_file, $identity_col, $engine, $batch_size) = @_;

    open(my $fh, '<', $in_file) or die "Can't open $in_file: $!\n";
    my $header_line = <$fh>;
    my $header = decode_json($header_line);
    die "$in_file: missing header line, is this a valid snapshot?\n"
        unless $header->{_header};

    my @cols = @{ $header->{columns} };
    print "Restoring $table ($in_file): " . scalar(@cols) . " columns expected\n";

    $dbh->do("TRUNCATE TABLE $table");

    # Sybase requires explicit permission to insert into identity columns;
    # Postgres allows it by default as long as you supply a value.
    if ($engine eq 'sybase' && $identity_col) {
        $dbh->do("SET IDENTITY_INSERT $table ON");
    }

    my $placeholders = join(', ', ('?') x scalar(@cols));
    my $col_list     = join(', ', @cols);
    my $insert_sth   = $dbh->prepare("INSERT INTO $table ($col_list) VALUES ($placeholders)");

    my $count = 0;
    my $footer;
    while (my $line = <$fh>) {
        chomp $line;
        my $decoded = decode_json($line);

        if (ref($decoded) eq 'HASH' && $decoded->{_footer}) {
            $footer = $decoded;
            last;
        }

        $insert_sth->execute(@$decoded);
        $count++;

        if ($count % $batch_size == 0) {
            print "  ...$count rows loaded\n";
        }
    }
    close $fh;

    if ($engine eq 'sybase' && $identity_col) {
        $dbh->do("SET IDENTITY_INSERT $table OFF");
        # Reseed so the next auto-generated identity value continues past
        # the max value just loaded, rather than restarting from wherever
        # it was before the truncate.
        $dbh->do("DBCC CHECKIDENT ($table, RESEED)");
    }

    if ($engine eq 'postgres' && $identity_col) {
        my ($seq) = $dbh->selectrow_array(
            "SELECT pg_get_serial_sequence(?, ?)", undef, $table, $identity_col
        );
        if ($seq) {
            $dbh->do("SELECT setval(?, COALESCE((SELECT MAX($identity_col) FROM $table), 1))", undef, $seq);
        } else {
            warn "  note: no serial sequence found for $table.$identity_col, skipping resync\n";
        }
    }

    if ($footer && $footer->{row_count} != $count) {
        warn "WARNING: snapshot claimed $footer->{row_count} rows but restored $count\n";
    }

    print "Restore complete: $count rows loaded into $table\n";
}

sub usage {
    return <<'USAGE';
Usage:
  ./snapshot_table.pl snapshot --config db_config.pl --engine sybase|postgres --table NAME --out FILE
  ./snapshot_table.pl restore  --config db_config.pl --engine sybase|postgres --table NAME --in FILE [--identity-col COL]
USAGE
}

__END__

=head1 CONFIG FILE (db_config.pl)

Same format used by compare_tables.pl — one shared file makes sense if
you're already using that script:

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

=head1 SNAPSHOT FILE FORMAT

JSON-lines:
  line 1:        {"_header":1,"table":"orders","columns":["order_id","customer_id",...]}
  lines 2..n+1:  [123,456,"shipped",...]     (one array per row, column order matches header)
  last line:     {"_footer":1,"row_count":n}

Chosen over CSV/bcp format specifically because it:
  - preserves NULL vs empty string unambiguously
  - handles embedded delimiters, newlines, and quotes in text columns
    without any escaping scheme to get wrong
  - is engine-agnostic — same snapshot format whether the source was
    Sybase or Postgres

=head1 TYPICAL WORKFLOW

  # Snapshot the table from wherever it currently has good baseline data
  ./snapshot_table.pl snapshot --config db_config.pl --engine sybase \
      --table orders --out snapshots/orders.snap

  # ... run your test script against sybase, which mutates the table ...

  # Restore it back to the pre-test state before the next run
  ./snapshot_table.pl restore --config db_config.pl --engine sybase \
      --table orders --in snapshots/orders.snap --identity-col order_id

  # Same snapshot file can seed Postgres too, if column names/order match
  ./snapshot_table.pl restore --config db_config.pl --engine postgres \
      --table orders --in snapshots/orders.snap --identity-col order_id

=head1 NOTES

- --identity-col is optional but recommended whenever the table has an
  auto-generated primary key and the script under test inserts new rows.
  Without it, a fresh insert after restore could get a different
  auto-generated ID on Sybase vs Postgres, which would then show up as a
  false mismatch in compare_tables.pl.
- TRUNCATE is used rather than DELETE for speed; if your table is
  referenced by foreign keys, you may need to disable/re-enable
  constraints around the restore, or add "CASCADE" (Postgres) — adjust
  the TRUNCATE line for your schema if needed.
- For very large tables (many millions of rows), restore performance will
  be limited by row-at-a-time INSERT via DBI. Consider batching with
  execute_array (DBI) or falling back to bcp/COPY (as in
  restore_baseline.sh) for tables where that matters.
- This script handles ONE table at a time. For resetting an entire
  baseline across many tables, use restore_baseline.sh instead — this
  script is for quick single-table snapshot/reset during iterative
  testing.

=cut
