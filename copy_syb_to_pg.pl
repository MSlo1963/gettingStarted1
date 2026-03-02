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
    port   => 5000,
    dbname => '',
    user   => 'sa',
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
my $tgt_table   = '';
my $src_schema  = 'dbo';       # Sybase owner / schema
my $tgt_schema  = 'public';    # PostgreSQL target schema
my $batch_size  = 1000;
my $drop_first  = 0;
my $data_only   = 0;
my $struct_only = 0;
my $help        = 0;

GetOptions(
    'src-host=s'    => \$src{host},
    'src-port=i'    => \$src{port},
    'src-db=s'      => \$src{dbname},
    'src-user=s'    => \$src{user},
    'src-pass=s'    => \$src{pass},
    'tgt-host=s'    => \$tgt{host},
    'tgt-port=i'    => \$tgt{port},
    'tgt-db=s'      => \$tgt{dbname},
    'tgt-user=s'    => \$tgt{user},
    'tgt-pass=s'    => \$tgt{pass},
    'table=s'       => \$table,
    'tgt-table=s'   => \$tgt_table,
    'src-schema=s'  => \$src_schema,
    'tgt-schema=s'  => \$tgt_schema,
    'batch=i'       => \$batch_size,
    'drop'          => \$drop_first,
    'data-only'     => \$data_only,
    'struct-only'   => \$struct_only,
    'help'          => \$help,
) or usage();

usage() if $help;

# ─────────────────────────────────────────────
#  Validate required args
# ─────────────────────────────────────────────
die "[ERROR] --table is required\n"  unless $table;
die "[ERROR] --src-db is required\n" unless $src{dbname};
die "[ERROR] --tgt-db is required\n" unless $tgt{dbname};

# Default target table name to source table name if not specified
$tgt_table = $table unless $tgt_table;

# ─────────────────────────────────────────────
#  Connect to Sybase (source)
# ─────────────────────────────────────────────
log_info("Connecting to Sybase source: $src{dbname} on $src{host}:$src{port}");
my $src_dbh = DBI->connect(
    "dbi:Sybase:server=$src{host};port=$src{port};database=$src{dbname}",
    $src{user}, $src{pass},
    { RaiseError => 1, AutoCommit => 1, PrintError => 0 }
) or die "[ERROR] Cannot connect to Sybase source: $DBI::errstr\n";

# Use the correct database explicitly
$src_dbh->do("USE $src{dbname}");

# ─────────────────────────────────────────────
#  Connect to PostgreSQL (target)
# ─────────────────────────────────────────────
log_info("Connecting to PostgreSQL target: $tgt{dbname} on $tgt{host}:$tgt{port}");
my $tgt_dbh = DBI->connect(
    "dbi:Pg:dbname=$tgt{dbname};host=$tgt{host};port=$tgt{port}",
    $tgt{user}, $tgt{pass},
    { RaiseError => 1, AutoCommit => 0, PrintError => 0 }
) or die "[ERROR] Cannot connect to PostgreSQL target: $DBI::errstr\n";

# ─────────────────────────────────────────────
#  Main
# ─────────────────────────────────────────────
eval {
    unless ($data_only) {
        copy_structure($src_dbh, $tgt_dbh, $src_schema, $tgt_schema, $table, $tgt_table);
    }
    unless ($struct_only) {
        copy_data($src_dbh, $tgt_dbh, $src_schema, $tgt_schema, $table, $tgt_table);
    }
    $tgt_dbh->commit();
    log_info("Done. '$src_schema.$table' (Sybase) copied to '$tgt_schema.$tgt_table' (PostgreSQL) successfully.");
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
    my ($src_dbh, $tgt_dbh, $src_schema, $tgt_schema, $table, $tgt_table) = @_;
    log_info("Reading column definitions for '$src_schema.$table' from Sybase ...");

    # ── Fetch column info from Sybase syscolumns / systypes ──────────────────
    # We query syscolumns joined to systypes because Sybase's information_schema
    # support is limited and does not always expose user-defined type names or
    # precise length/scale for all types.
    my $col_sth = $src_dbh->prepare(qq{
        SELECT
            c.name                          AS column_name,
            t.name                          AS type_name,
            c.length                        AS col_length,
            c.prec                          AS col_prec,
            c.scale                         AS col_scale,
            c.status                        AS col_status,
            c.colid                         AS col_order,
            CASE WHEN c.status & 8 = 8
                 THEN 1 ELSE 0 END          AS is_identity,
            c.cdefault                      AS default_id
        FROM $src{dbname}..syscolumns c
        JOIN $src{dbname}..systypes   t  ON c.usertype = t.usertype
        WHERE c.id = OBJECT_ID('$src_schema.$table')
        ORDER BY c.colid
    });
    $col_sth->execute();
    my $columns = $col_sth->fetchall_arrayref({});
    die "[ERROR] Table '$src_schema.$table' not found in Sybase source DB.\n" unless @$columns;

    # ── Fetch primary key columns ─────────────────────────────────────────────
    my $pk_sth = $src_dbh->prepare(qq{
        SELECT c.name
        FROM $src{dbname}..sysindexes  i
        JOIN $src{dbname}..sysindexkeys ik ON i.id      = ik.id
                                          AND i.indid   = ik.indid
        JOIN $src{dbname}..syscolumns  c  ON ik.id      = c.id
                                          AND ik.colid  = c.colid
        WHERE i.id     = OBJECT_ID('$src_schema.$table')
          AND i.status & 2048 = 2048
        ORDER BY ik.keyno
    });
    $pk_sth->execute();
    my @pk_cols;
    while (my $row = $pk_sth->fetchrow_arrayref()) {
        push @pk_cols, qq("$row->[0]");
    }

    # ── Build PostgreSQL CREATE TABLE DDL ────────────────────────────────────
    my @col_defs;
    for my $col (@$columns) {
        my $name     = qq("$col->{column_name}");
        my $pg_type  = sybase_to_pg_type($col);
        # status bit 8 = identity column → use SERIAL / BIGSERIAL
        if ($col->{is_identity}) {
            $pg_type = ($pg_type =~ /^bigint$/i) ? 'BIGSERIAL' : 'SERIAL';
        }
        # status bit 1 = NOT NULL (0x1), also NOT NULL when identity
        my $nullable = ($col->{col_status} & 1 || $col->{is_identity}) ? ' NOT NULL' : '';
        push @col_defs, "    $name $pg_type$nullable";
    }

    push @col_defs, "    PRIMARY KEY (" . join(', ', @pk_cols) . ")" if @pk_cols;

    # ── Drop existing PG table if requested ──────────────────────────────────
    if ($drop_first) {
        log_info("Dropping existing table '$tgt_schema.$tgt_table' on target (--drop) ...");
        $tgt_dbh->do(qq{DROP TABLE IF EXISTS "$tgt_schema"."$tgt_table" CASCADE});
    }

    my $ddl = qq{CREATE TABLE IF NOT EXISTS "$tgt_schema"."$tgt_table" (\n}
            . join(",\n", @col_defs)
            . "\n)";

    log_info("Creating table '$tgt_schema.$tgt_table' on PostgreSQL target ...");
    log_debug($ddl);
    $tgt_dbh->do($ddl);

    # ── Copy non-PK indexes ───────────────────────────────────────────────────
    copy_indexes($src_dbh, $tgt_dbh, $src_schema, $tgt_schema, $table, $tgt_table);

    $tgt_dbh->commit();
    log_info("Structure copied.");
}

# ─────────────────────────────────────────────
#  Sybase → PostgreSQL type mapping
# ─────────────────────────────────────────────
sub sybase_to_pg_type {
    my ($col) = @_;
    my $type  = lc($col->{type_name});
    my $len   = $col->{col_length};
    my $prec  = $col->{col_prec};
    my $scale = $col->{col_scale};

    # ── String types ──────────────────────────────────────────────────────────
    return "VARCHAR($len)"   if $type eq 'varchar'  && $len;
    return 'TEXT'            if $type eq 'varchar';
    return "CHAR($len)"      if $type eq 'char'     && $len;
    return 'CHAR(1)'         if $type eq 'char';
    return 'TEXT'            if $type =~ /^(text|unitext|longsysname)$/;
    return "NVARCHAR($len)"  if $type eq 'nvarchar'  && $len;   # PG supports nvarchar as alias
    return 'TEXT'            if $type eq 'nvarchar';
    return "NCHAR($len)"     if $type eq 'nchar'    && $len;
    return 'TEXT'            if $type eq 'nchar';

    # ── Numeric / integer types ───────────────────────────────────────────────
    return 'SMALLINT'        if $type =~ /^(smallint|tinyint)$/;    # tinyint → smallint (PG has no tinyint)
    return 'INTEGER'         if $type eq 'int';
    return 'BIGINT'          if $type eq 'bigint';
    return 'REAL'            if $type =~ /^(real|float4)$/;
    return 'DOUBLE PRECISION' if $type =~ /^(float|double precision|float8)$/;
    if ($type =~ /^(numeric|decimal)$/) {
        return (defined $prec && defined $scale) ? "NUMERIC($prec,$scale)" : 'NUMERIC';
    }
    return 'NUMERIC(19,4)'   if $type eq 'money';
    return 'NUMERIC(10,4)'   if $type eq 'smallmoney';

    # ── Date / time types ─────────────────────────────────────────────────────
    return 'TIMESTAMP'       if $type =~ /^(datetime|smalldatetime|bigdatetime)$/;
    return 'TIME'            if $type eq 'bigtime';
    return 'DATE'            if $type eq 'date';
    return 'TIME'            if $type eq 'time';

    # ── Binary / other types ──────────────────────────────────────────────────
    return 'BYTEA'           if $type =~ /^(binary|varbinary|image|timestamp)$/;
    return 'BOOLEAN'         if $type eq 'bit';
    return 'TEXT'            if $type eq 'sysname';

    # ── Fallback – keep the type name and let PostgreSQL complain if needed ───
    log_warn("Unknown Sybase type '$type' – passing through as-is; manual review recommended.");
    return $type;
}

# ─────────────────────────────────────────────
#  Copy non-PK indexes from Sybase → PostgreSQL
# ─────────────────────────────────────────────
sub copy_indexes {
    my ($src_dbh, $tgt_dbh, $src_schema, $tgt_schema, $table, $tgt_table) = @_;

    # Fetch non-clustered, non-primary-key indexes from Sybase sysindexes
    my $idx_sth = $src_dbh->prepare(qq{
        SELECT
            i.name                              AS index_name,
            i.status                            AS index_status,
            c.name                              AS col_name,
            ik.keyno                            AS key_order
        FROM $src{dbname}..sysindexes   i
        JOIN $src{dbname}..sysindexkeys ik ON i.id    = ik.id
                                          AND i.indid = ik.indid
        JOIN $src{dbname}..syscolumns   c  ON ik.id   = c.id
                                          AND ik.colid = c.colid
        WHERE i.id      = OBJECT_ID('$src_schema.$table')
          AND i.indid  >= 1
          AND i.status & 2048 = 0       -- exclude primary key indexes
          AND i.status & 32  = 0        -- exclude hypothetical indexes
        ORDER BY i.name, ik.keyno
    });
    $idx_sth->execute();

    # Group columns per index
    my %indexes;
    my @index_order;
    while (my $row = $idx_sth->fetchrow_hashref()) {
        unless (exists $indexes{ $row->{index_name} }) {
            push @index_order, $row->{index_name};
            $indexes{ $row->{index_name} } = {
                unique  => ($row->{index_status} & 2) ? 1 : 0,
                columns => [],
            };
        }
        push @{ $indexes{ $row->{index_name} }{columns} }, $row->{col_name};
    }

    my $count = 0;
    for my $idx_name (@index_order) {
        my $info       = $indexes{$idx_name};
        my $unique     = $info->{unique} ? 'UNIQUE ' : '';
        my $col_list   = join(', ', map { qq("$_") } @{ $info->{columns} });
        # Build a safe PG index name: prefix with tgt_table to avoid conflicts
        (my $safe_idx  = "${tgt_table}_${idx_name}") =~ s/\W/_/g;
        my $pg_idx_ddl = qq{CREATE ${unique}INDEX IF NOT EXISTS "$safe_idx" }
                       . qq{ON "$tgt_schema"."$tgt_table" ($col_list)};
        log_debug($pg_idx_ddl);
        eval { $tgt_dbh->do($pg_idx_ddl) };
        if ($@) {
            log_warn("Could not create index '$safe_idx' (may already exist): $@");
        } else {
            $count++;
        }
    }
    log_info("Copied $count index(es).") if $count;
}

# ─────────────────────────────────────────────
#  Copy data Sybase → PostgreSQL
# ─────────────────────────────────────────────
sub copy_data {
    my ($src_dbh, $tgt_dbh, $src_schema, $tgt_schema, $table, $tgt_table) = @_;

    # Row count from Sybase
    my ($total) = $src_dbh->selectrow_array(
        qq{SELECT COUNT(*) FROM $src_schema.$table}
    );
    log_info("Copying $total row(s) from Sybase '$src_schema.$table' "
           . "to PostgreSQL '$tgt_schema.$tgt_table' in batches of $batch_size ...");

    return if $total == 0;

    # Fetch column names in ordinal order from Sybase
    my $col_sth = $src_dbh->prepare(qq{
        SELECT c.name
        FROM $src{dbname}..syscolumns c
        WHERE c.id = OBJECT_ID('$src_schema.$table')
        ORDER BY c.colid
    });
    $col_sth->execute();
    my @col_names = map { $_->[0] } @{ $col_sth->fetchall_arrayref() };

    # Sybase SELECT: plain column names (no quoting needed for standard names)
    my $src_col_list = join(', ', @col_names);
    # PostgreSQL INSERT: double-quoted column names
    my $tgt_col_list = join(', ', map { qq("$_") } @col_names);
    my $ph_list      = join(', ', ('?') x scalar @col_names);

    my $select_sql = qq{SELECT $src_col_list FROM $src_schema.$table};
    my $insert_sql = qq{INSERT INTO "$tgt_schema"."$tgt_table" ($tgt_col_list) VALUES ($ph_list)
                        ON CONFLICT DO NOTHING};

    log_debug("SELECT: $select_sql");
    log_debug("INSERT: $insert_sql");

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
sub log_info  { print "[INFO]  $_[0]\n" }
sub log_warn  { print "[WARN]  $_[0]\n" }
sub log_error { print "[ERROR] $_[0]\n" }
sub log_debug { print "[DEBUG] $_[0]\n" }

# ─────────────────────────────────────────────
#  Usage
# ─────────────────────────────────────────────
sub usage {
    print <<'END';
Usage: perl copy_sybase_to_pg.pl [OPTIONS]

Copies a table from a Sybase ASE database into a PostgreSQL database.
Schema structure (columns, primary key, indexes) is translated automatically.

Required:
  --table=NAME        Source table name in Sybase
  --src-db=NAME       Sybase source database name
  --tgt-db=NAME       PostgreSQL target database name

Source – Sybase ASE (defaults: localhost:5000, user=sa):
  --src-host=HOST
  --src-port=PORT
  --src-user=USER
  --src-pass=PASS
  --src-schema=NAME   Sybase owner/schema (default: dbo)

Target – PostgreSQL (defaults: localhost:5432, user=postgres):
  --tgt-host=HOST
  --tgt-port=PORT
  --tgt-user=USER
  --tgt-pass=PASS
  --tgt-schema=NAME   PostgreSQL schema (default: public)
  --tgt-table=NAME    Target table name (default: same as --table)

Options:
  --batch=N           Insert batch size (default: 1000)
  --drop              DROP the target table before creating it
  --data-only         Copy data only (skip CREATE TABLE)
  --struct-only       Copy structure/indexes only (no data)
  --help              Show this help

Sybase → PostgreSQL type mapping:
  varchar(n)          → VARCHAR(n)
  char(n)             → CHAR(n)
  text / unitext      → TEXT
  nvarchar(n)         → NVARCHAR(n)
  tinyint             → SMALLINT
  smallint            → SMALLINT
  int                 → INTEGER
  bigint              → BIGINT
  float / double      → DOUBLE PRECISION
  real                → REAL
  numeric(p,s)        → NUMERIC(p,s)
  money               → NUMERIC(19,4)
  smallmoney          → NUMERIC(10,4)
  datetime            → TIMESTAMP
  smalldatetime       → TIMESTAMP
  date                → DATE
  time                → TIME
  binary / varbinary  → BYTEA
  image               → BYTEA
  bit                 → BOOLEAN
  identity column     → SERIAL / BIGSERIAL

Prerequisites:
  cpan DBD::Sybase   (requires FreeTDS or Sybase Open Client libraries)
  cpan DBD::Pg

Examples:
  # Full copy (structure + data), same table name
  perl copy_sybase_to_pg.pl \
    --src-host=sybase.example.com --src-db=PROD --src-user=sa --src-pass=secret \
    --tgt-host=pg.example.com     --tgt-db=prod --tgt-user=postgres --tgt-pass=secret \
    --table=orders

  # Copy with a different target table name and schema
  perl copy_sybase_to_pg.pl \
    --src-host=sybase.example.com --src-db=PROD --src-user=sa --src-pass=secret \
    --tgt-host=pg.example.com     --tgt-db=prod --tgt-user=postgres --tgt-pass=secret \
    --table=customers --src-schema=sales --tgt-schema=public --tgt-table=customers_migrated

  # Structure only – useful to inspect the DDL before copying data
  perl copy_sybase_to_pg.pl \
    --src-db=PROD --tgt-db=prod --table=orders --struct-only

  # Re-create target table from scratch, then copy data
  perl copy_sybase_to_pg.pl \
    --src-db=PROD --tgt-db=prod --table=orders --drop

  # Data only – target table already exists
  perl copy_sybase_to_pg.pl \
    --src-db=PROD --tgt-db=prod --table=orders --data-only --batch=5000

END
    exit 0;
}
