#!/usr/bin/perl
use strict;
use warnings;
use DBI;
use Getopt::Long;
use POSIX qw(ceil);

# ─────────────────────────────────────────────
#  Configuration – override via CLI flags
# ─────────────────────────────────────────────
my %src = (
    host   => 'localhost',
    port   => 5432,
    dbname => '',
    user   => 'postgres',
    pass   => '',
);

my %tgt = (
    host   => 'localhost',
    port   => 5432,
    dbname => '',
    user   => 'postgres',
    pass   => '',
);

my $table       = '';
my $schema      = 'public';
my $batch_size  = 1000;
my $drop_first  = 0;
my $data_only   = 0;
my $struct_only = 0;
my $help        = 0;

GetOptions(
    'src-host=s'   => \$src{host},
    'src-port=i'   => \$src{port},
    'src-db=s'     => \$src{dbname},
    'src-user=s'   => \$src{user},
    'src-pass=s'   => \$src{pass},
    'tgt-host=s'   => \$tgt{host},
    'tgt-port=i'   => \$tgt{port},
    'tgt-db=s'     => \$tgt{dbname},
    'tgt-user=s'   => \$tgt{user},
    'tgt-pass=s'   => \$tgt{pass},
    'table=s'      => \$table,
    'schema=s'     => \$schema,
    'batch=i'      => \$batch_size,
    'drop'         => \$drop_first,
    'data-only'    => \$data_only,
    'struct-only'  => \$struct_only,
    'help'         => \$help,
) or usage();

usage() if $help;

# ─────────────────────────────────────────────
#  Validate required args
# ─────────────────────────────────────────────
die "[ERROR] --table is required\n"  unless $table;
die "[ERROR] --src-db is required\n" unless $src{dbname};
die "[ERROR] --tgt-db is required\n" unless $tgt{dbname};

# ─────────────────────────────────────────────
#  Connect to both databases
# ─────────────────────────────────────────────
log_info("Connecting to source DB: $src{dbname} on $src{host}:$src{port}");
my $src_dbh = DBI->connect(
    "dbi:Pg:dbname=$src{dbname};host=$src{host};port=$src{port}",
    $src{user}, $src{pass},
    { RaiseError => 1, AutoCommit => 1, PrintError => 0 }
) or die "[ERROR] Cannot connect to source: $DBI::errstr\n";

log_info("Connecting to target DB: $tgt{dbname} on $tgt{host}:$tgt{port}");
my $tgt_dbh = DBI->connect(
    "dbi:Pg:dbname=$tgt{dbname};host=$tgt{host};port=$tgt{port}",
    $tgt{user}, $tgt{pass},
    { RaiseError => 1, AutoCommit => 0, PrintError => 0 }
) or die "[ERROR] Cannot connect to target: $DBI::errstr\n";

# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────
eval {
    unless ($data_only) {
        copy_structure($src_dbh, $tgt_dbh, $schema, $table);
    }
    unless ($struct_only) {
        copy_data($src_dbh, $tgt_dbh, $schema, $table);
    }
    $tgt_dbh->commit();
    log_info("Done. Table '$schema.$table' copied successfully.");
};
if ($@) {
    log_error("Rolling back due to error: $@");
    eval { $tgt_dbh->rollback() };
    exit 1;
}

$src_dbh->disconnect();
$tgt_dbh->disconnect();

# ─────────────────────────────────────────────
#  Subroutines
# ─────────────────────────────────────────────

sub copy_structure {
    my ($src_dbh, $tgt_dbh, $schema, $table) = @_;
    log_info("Reading column definitions for '$schema.$table' ...");

    # Fetch column info
    my $col_sth = $src_dbh->prepare(q{
        SELECT
            column_name,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale,
            is_nullable,
            column_default,
            udt_name
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name   = ?
        ORDER BY ordinal_position
    });
    $col_sth->execute($schema, $table);
    my $columns = $col_sth->fetchall_arrayref({});
    die "[ERROR] Table '$schema.$table' not found in source DB.\n" unless @$columns;

    # Build CREATE TABLE DDL
    my @col_defs;
    for my $col (@$columns) {
        my $name     = qq("$col->{column_name}");
        my $type     = resolve_type($col);
        my $nullable = ($col->{is_nullable} eq 'NO') ? ' NOT NULL' : '';
        my $default  = '';
        if (defined $col->{column_default} && $col->{column_default} !~ /nextval/i) {
            $default = " DEFAULT $col->{column_default}";
        }
        push @col_defs, "    $name $type$default$nullable";
    }

    # Fetch primary key
    my $pk_sth = $src_dbh->prepare(q{
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema    = kcu.table_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema    = ?
          AND tc.table_name      = ?
        ORDER BY kcu.ordinal_position
    });
    $pk_sth->execute($schema, $table);
    my @pk_cols;
    while (my $row = $pk_sth->fetchrow_arrayref()) {
        push @pk_cols, qq("$row->[0]");
    }
    push @col_defs, "    PRIMARY KEY (" . join(', ', @pk_cols) . ")" if @pk_cols;

    # Drop existing table if requested
    if ($drop_first) {
        log_info("Dropping existing table '$schema.$table' on target (--drop) ...");
        $tgt_dbh->do(qq{DROP TABLE IF EXISTS "$schema"."$table" CASCADE});
    }

    my $ddl = qq{CREATE TABLE IF NOT EXISTS "$schema"."$table" (\n}
            . join(",\n", @col_defs)
            . "\n)";

    log_info("Creating table on target ...");
    log_debug($ddl);
    $tgt_dbh->do($ddl);

    # Copy indexes (non-PK)
    copy_indexes($src_dbh, $tgt_dbh, $schema, $table);

    $tgt_dbh->commit();
    log_info("Structure copied.");
}

sub resolve_type {
    my ($col) = @_;
    my $dtype = lc($col->{data_type});

    # Prefer udt_name for arrays / custom / enum types
    if ($dtype eq 'array') {
        return "$col->{udt_name}[]";
    }
    if ($dtype eq 'user-defined') {
        return $col->{udt_name};
    }
    if ($dtype =~ /character varying/i || $dtype eq 'varchar') {
        my $len = $col->{character_maximum_length} // '';
        return $len ? "VARCHAR($len)" : "TEXT";
    }
    if ($dtype eq 'character' || $dtype eq 'char') {
        my $len = $col->{character_maximum_length} // 1;
        return "CHAR($len)";
    }
    if ($dtype eq 'numeric' || $dtype eq 'decimal') {
        my ($p, $s) = ($col->{numeric_precision}, $col->{numeric_scale});
        return (defined $p && defined $s) ? "NUMERIC($p,$s)" : "NUMERIC";
    }
    # Pass through everything else (int, text, boolean, date, timestamp, etc.)
    return $dtype;
}

sub copy_indexes {
    my ($src_dbh, $tgt_dbh, $schema, $table) = @_;

    my $idx_sth = $src_dbh->prepare(q{
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = ?
          AND tablename  = ?
          AND indexname NOT IN (
              SELECT constraint_name
              FROM information_schema.table_constraints
              WHERE table_schema = ?
                AND table_name   = ?
                AND constraint_type = 'PRIMARY KEY'
          )
    });
    $idx_sth->execute($schema, $table, $schema, $table);

    my $count = 0;
    while (my ($indexdef) = $idx_sth->fetchrow_array()) {
        eval { $tgt_dbh->do($indexdef) };
        if ($@) {
            log_warn("Could not create index (may already exist): $@");
        } else {
            $count++;
        }
    }
    log_info("Copied $count index(es).") if $count;
}

sub copy_data {
    my ($src_dbh, $tgt_dbh, $schema, $table) = @_;

    # Count rows
    my ($total) = $src_dbh->selectrow_array(
        qq{SELECT COUNT(*) FROM "$schema"."$table"}
    );
    log_info("Copying $total row(s) in batches of $batch_size ...");

    return if $total == 0;

    # Fetch column names in order
    my $col_sth = $src_dbh->prepare(q{
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name   = ?
        ORDER BY ordinal_position
    });
    $col_sth->execute($schema, $table);
    my @col_names = map { $_->[0] } @{ $col_sth->fetchall_arrayref() };
    my $col_list  = join(', ', map { qq("$_") } @col_names);
    my $ph_list   = join(', ', ('?') x scalar @col_names);

    my $select_sql = qq{SELECT $col_list FROM "$schema"."$table"};
    my $insert_sql = qq{INSERT INTO "$schema"."$table" ($col_list) VALUES ($ph_list)
                        ON CONFLICT DO NOTHING};

    my $sel_sth = $src_dbh->prepare($select_sql);
    my $ins_sth = $tgt_dbh->prepare($insert_sql);

    $sel_sth->execute();

    my $inserted = 0;
    my $batches  = 0;
    my @batch;

    while (my $row = $sel_sth->fetchrow_arrayref()) {
        push @batch, [@$row];
        if (@batch >= $batch_size) {
            flush_batch($ins_sth, \@batch, \$inserted);
            @batch = ();
            $batches++;
            log_info(sprintf("  Progress: %d / %d rows (%.1f%%)",
                $inserted, $total, ($inserted / $total) * 100));
        }
    }
    flush_batch($ins_sth, \@batch, \$inserted) if @batch;

    log_info("Data copy complete: $inserted row(s) inserted.");
}

sub flush_batch {
    my ($ins_sth, $batch, $inserted_ref) = @_;
    for my $row (@$batch) {
        $ins_sth->execute(@$row);
        $$inserted_ref++;
    }
}

# ─────────────────────────────────────────────
#  Logging helpers
# ─────────────────────────────────────────────
sub log_info  { print  "[INFO]  $_[0]\n" }
sub log_warn  { print  "[WARN]  $_[0]\n" }
sub log_error { print  "[ERROR] $_[0]\n" }
sub log_debug { print  "[DEBUG] $_[0]\n" }

# ─────────────────────────────────────────────
#  Usage
# ─────────────────────────────────────────────
sub usage {
    print <<'END';
Usage: perl copy_pg_table.pl [OPTIONS]

Required:
  --table=NAME        Table name to copy
  --src-db=NAME       Source database name
  --tgt-db=NAME       Target database name

Source connection (defaults: localhost:5432, user=postgres):
  --src-host=HOST
  --src-port=PORT
  --src-user=USER
  --src-pass=PASS

Target connection (defaults: localhost:5432, user=postgres):
  --tgt-host=HOST
  --tgt-port=PORT
  --tgt-user=USER
  --tgt-pass=PASS

Options:
  --schema=NAME       Schema name (default: public)
  --batch=N           Insert batch size (default: 1000)
  --drop              DROP the target table before creating it
  --data-only         Copy data only (skip CREATE TABLE)
  --struct-only       Copy structure/indexes only (no data)
  --help              Show this help

Examples:
  # Full copy (structure + data) on the same server
  perl copy_pg_table.pl --src-db=mydb --tgt-db=newdb --table=orders

  # Cross-server copy
  perl copy_pg_table.pl \
    --src-host=db1.example.com --src-db=prod --src-user=admin --src-pass=secret \
    --tgt-host=db2.example.com --tgt-db=staging --tgt-user=admin --tgt-pass=secret \
    --table=customers --schema=sales --batch=500

  # Re-create table from scratch and copy data
  perl copy_pg_table.pl --src-db=mydb --tgt-db=newdb --table=orders --drop

END
    exit 0;
}
