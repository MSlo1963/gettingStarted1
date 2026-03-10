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
    port   => 5000,
    dbname => '',
    user   => 'sa',
    pass   => '',
);

my $table       = '';
my $tgt_table   = '';
my $src_schema  = 'dbo';
my $tgt_schema  = 'dbo';
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
#  Connect to source Sybase
# ─────────────────────────────────────────────
log_info("Connecting to source Sybase: $src{dbname} on $src{host}:$src{port}");
my $src_dbh = DBI->connect(
    "dbi:Sybase:server=$src{host};port=$src{port};database=$src{dbname}",
    $src{user}, $src{pass},
    { RaiseError => 1, AutoCommit => 1, PrintError => 0 }
) or die "[ERROR] Cannot connect to source Sybase: $DBI::errstr\n";

$src_dbh->do("USE $src{dbname}");

# ─────────────────────────────────────────────
#  Connect to target Sybase
# ─────────────────────────────────────────────
log_info("Connecting to target Sybase: $tgt{dbname} on $tgt{host}:$tgt{port}");
my $tgt_dbh = DBI->connect(
    "dbi:Sybase:server=$tgt{host};port=$tgt{port};database=$tgt{dbname}",
    $tgt{user}, $tgt{pass},
    { RaiseError => 1, AutoCommit => 1, PrintError => 0 }
) or die "[ERROR] Cannot connect to target Sybase: $DBI::errstr\n";

$tgt_dbh->do("USE $tgt{dbname}");

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
    log_info("Done. '$src_schema.$table' (Sybase $src{dbname}) copied to '$tgt_schema.$tgt_table' (Sybase $tgt{dbname}) successfully.");
};
if ($@) {
    log_error("Failed: $@");
    exit 1;
}

$src_dbh->disconnect();
$tgt_dbh->disconnect();

# ─────────────────────────────────────────────
#  Subroutines
# ─────────────────────────────────────────────

sub copy_structure {
    my ($src_dbh, $tgt_dbh, $src_schema, $tgt_schema, $table, $tgt_table) = @_;
    log_info("Reading column definitions for '$src_schema.$table' from source Sybase ...");

    # ── Fetch column info from syscolumns / systypes ──────────────────────────
    my $col_sth = $src_dbh->prepare(qq{
        SELECT
            c.name                              AS column_name,
            t.name                              AS type_name,
            c.length                            AS col_length,
            c.prec                              AS col_prec,
            c.scale                             AS col_scale,
            c.status                            AS col_status,
            c.colid                             AS col_order,
            CASE WHEN c.status & 16 = 16
                 THEN 1 ELSE 0 END              AS is_identity,
            c.cdefault                          AS default_id
        FROM $src{dbname}..syscolumns c
        JOIN $src{dbname}..systypes   t  ON c.usertype = t.usertype
        WHERE c.id = OBJECT_ID('$src_schema.$table')
        ORDER BY c.colid
    });
    $col_sth->execute();
    my $columns = $col_sth->fetchall_arrayref({});
    die "[ERROR] Table '$src_schema.$table' not found in source Sybase DB '$src{dbname}'.\n"
        unless @$columns;

    # ── Fetch default values from syscomments ────────────────────────────────
    # cdefault holds the id of the default object; look up the actual text
    my %defaults;
    for my $col (@$columns) {
        next unless $col->{default_id};
        my ($def_text) = $src_dbh->selectrow_array(qq{
            SELECT c.text
            FROM   $src{dbname}..syscomments c
            WHERE  c.id = $col->{default_id}
        });
        # syscomments stores the full "DEFAULT expr" text – extract just the value
        if (defined $def_text) {
            $def_text =~ s/^\s+|\s+$//g;               # trim
            $defaults{ $col->{column_name} } = $def_text;
        }
    }

    # ── Fetch primary key index ───────────────────────────────────────────────
    # sysindexkeys does not exist in Sybase ASE.
    # Use index_col(table, indid, keyno) + sysindexes.keycnt instead.
    my $pk_idx_sth = $src_dbh->prepare(qq{
        SELECT i.indid, i.keycnt
        FROM   $src{dbname}..sysindexes i
        WHERE  i.id     = OBJECT_ID('$src_schema.$table')
          AND  i.status & 2048 = 2048
    });
    $pk_idx_sth->execute();
    my @pk_cols;
    while (my $pk_row = $pk_idx_sth->fetchrow_hashref()) {
        my $indid  = $pk_row->{indid};
        my $keycnt = $pk_row->{keycnt};
        for my $keyno (1 .. $keycnt) {
            my ($col_name) = $src_dbh->selectrow_array(
                qq{SELECT index_col('$src_schema.$table', $indid, $keyno)}
            );
            last unless defined $col_name;
            push @pk_cols, $col_name;
        }
    }

    # ── Build Sybase CREATE TABLE DDL ─────────────────────────────────────────
    my @col_defs;
    for my $col (@$columns) {
        my $name    = $col->{column_name};
        my $type    = sybase_native_type($col);

        # IDENTITY columns
        if ($col->{is_identity}) {
            # Fetch the identity seed and increment from the source
            my ($seed, $incr) = $src_dbh->selectrow_array(qq{
                SELECT ident_seed('$src_schema.$table'),
                       ident_incr('$src_schema.$table')
            });
            $seed //= 1;
            $incr //= 1;
            push @col_defs, "    $name $type IDENTITY($seed,$incr) NOT NULL";
            next;
        }

        # TIMESTAMP columns – Sybase manages this as a binary row-version counter.
        # No DEFAULT and no explicit NULL/NOT NULL – just declare the type and let
        # Sybase handle everything.  Data copy skips these columns entirely.
        if (lc($type) eq 'timestamp') {
            log_info("  Column '$name' is timestamp – declared as-is, will be skipped during data copy.");
            push @col_defs, "    $name timestamp NOT NULL";
            next;
        }

        # NULL / NOT NULL
        # status bit 0x01 = nulls ARE allowed
        my $nullable = ($col->{col_status} & 1) ? ' NULL' : ' NOT NULL';

        # DEFAULT value (if any)
        my $default = '';
        if (exists $defaults{$name}) {
            $default = " DEFAULT $defaults{$name}";
        }

        push @col_defs, "    $name $type$default$nullable";
    }

    # Primary key constraint inline
    if (@pk_cols) {
        my $pk_col_list = join(', ', @pk_cols);
        push @col_defs, "    PRIMARY KEY ($pk_col_list)";
    }

    # ── Drop existing table on target if requested ────────────────────────────
    if ($drop_first) {
        log_info("Dropping existing table '$tgt_schema.$tgt_table' on target (--drop) ...");
        eval { $tgt_dbh->do("DROP TABLE $tgt_schema.$tgt_table") };
        if ($@) {
            log_warn("Could not drop '$tgt_schema.$tgt_table' (may not exist): $@");
        }
    }

    # ── Execute CREATE TABLE on target ────────────────────────────────────────
    my $ddl = "CREATE TABLE $tgt_schema.$tgt_table (\n"
            . join(",\n", @col_defs)
            . "\n)";

    log_info("Creating table '$tgt_schema.$tgt_table' on target Sybase ...");
    log_debug($ddl);
    eval { $tgt_dbh->do($ddl) };
    if ($@) {
        die "[ERROR] Could not create table '$tgt_schema.$tgt_table': $@\n";
    }

    # ── Copy non-PK indexes ───────────────────────────────────────────────────
    copy_indexes($src_dbh, $tgt_dbh, $src_schema, $tgt_schema, $table, $tgt_table);

    log_info("Structure copied.");
}

# ─────────────────────────────────────────────
#  Build the native Sybase type string
# ─────────────────────────────────────────────
sub sybase_native_type {
    my ($col)  = @_;
    my $type   = lc($col->{type_name});
    my $len    = $col->{col_length};
    my $prec   = $col->{col_prec};
    my $scale  = $col->{col_scale};

    # ── String types ──────────────────────────────────────────────────────────
    return "varchar($len)"          if $type eq 'varchar';
    return "char($len)"             if $type eq 'char';
    return "nvarchar($len)"         if $type eq 'nvarchar';
    return "nchar($len)"            if $type eq 'nchar';
    return 'text'                   if $type eq 'text';
    return 'unitext'                if $type eq 'unitext';

    # ── Numeric / integer types ───────────────────────────────────────────────
    return 'tinyint'                if $type eq 'tinyint';
    return 'smallint'               if $type eq 'smallint';
    return 'int'                    if $type eq 'int';
    return 'bigint'                 if $type eq 'bigint';
    return 'real'                   if $type eq 'real';
    return 'float'                  if $type eq 'float';
    return 'double precision'       if $type eq 'double precision';
    if ($type =~ /^(numeric|decimal)$/) {
        return (defined $prec && defined $scale)
            ? "${type}($prec,$scale)"
            : $type;
    }
    return 'money'                  if $type eq 'money';
    return 'smallmoney'             if $type eq 'smallmoney';

    # ── Date / time types ─────────────────────────────────────────────────────
    return 'datetime'               if $type eq 'datetime';
    return 'smalldatetime'          if $type eq 'smalldatetime';
    return 'bigdatetime'            if $type eq 'bigdatetime';
    return 'bigtime'                if $type eq 'bigtime';
    return 'date'                   if $type eq 'date';
    return 'time'                   if $type eq 'time';

    # ── Binary types ──────────────────────────────────────────────────────────
    return "binary($len)"           if $type eq 'binary';
    return "varbinary($len)"        if $type eq 'varbinary';
    return 'image'                  if $type eq 'image';

    # ── Other types ───────────────────────────────────────────────────────────
    return 'bit'                    if $type eq 'bit';
    return 'sysname'                if $type eq 'sysname';
    return 'timestamp'              if $type eq 'timestamp';

    # ── Fallback ──────────────────────────────────────────────────────────────
    log_warn("Unknown Sybase type '$type' – passing through as-is.");
    return $type;
}

# ─────────────────────────────────────────────
#  Copy non-PK indexes Sybase → Sybase
# ─────────────────────────────────────────────
sub copy_indexes {
    my ($src_dbh, $tgt_dbh, $src_schema, $tgt_schema, $table, $tgt_table) = @_;

    # Fetch all non-PK, non-text/image indexes from sysindexes
    my $idx_sth = $src_dbh->prepare(qq{
        SELECT
            i.name      AS index_name,
            i.status    AS index_status,
            i.status2   AS index_status2,
            i.indid     AS indid,
            i.keycnt    AS keycnt
        FROM $src{dbname}..sysindexes i
        WHERE i.id      = OBJECT_ID('$src_schema.$table')
          AND i.indid  >= 1
          AND i.indid  <> 255             -- exclude text/image rows
          AND i.status & 2048 = 0         -- exclude primary key indexes
          AND i.status & 32   = 0         -- exclude hypothetical indexes
        ORDER BY i.name
    });
    $idx_sth->execute();

    # Resolve column names via index_col() per index
    my %indexes;
    my @index_order;
    while (my $row = $idx_sth->fetchrow_hashref()) {
        my $idx_name = $row->{index_name};
        my $indid    = $row->{indid};
        my $keycnt   = $row->{keycnt};

        unless (exists $indexes{$idx_name}) {
            push @index_order, $idx_name;
            $indexes{$idx_name} = {
                unique    => ($row->{index_status}  &    2) ? 1 : 0,
                clustered => ($row->{index_status}  &   16) ? 0 : 1,  # bit 4 off = clustered
                columns   => [],
            };
        }

        for my $keyno (1 .. $keycnt) {
            my ($col_name) = $src_dbh->selectrow_array(
                qq{SELECT index_col('$src_schema.$table', $indid, $keyno)}
            );
            last unless defined $col_name;
            push @{ $indexes{$idx_name}{columns} }, $col_name;
        }
    }

    my $count = 0;
    for my $idx_name (@index_order) {
        my $info      = $indexes{$idx_name};
        my $unique    = $info->{unique}    ? 'UNIQUE '    : '';
        my $clustered = $info->{clustered} ? 'CLUSTERED ' : 'NONCLUSTERED ';
        my $col_list  = join(', ', @{ $info->{columns} });

        # Build Sybase CREATE INDEX syntax
        my $ddl = "CREATE ${unique}${clustered}INDEX $idx_name "
                . "ON $tgt_schema.$tgt_table ($col_list)";

        log_debug($ddl);
        eval { $tgt_dbh->do($ddl) };
        if ($@) {
            log_warn("Could not create index '$idx_name' (may already exist): $@");
        } else {
            $count++;
        }
    }
    log_info("Copied $count index(es).") if $count;
}

# ─────────────────────────────────────────────
#  Copy data Sybase → Sybase
# ─────────────────────────────────────────────
sub copy_data {
    my ($src_dbh, $tgt_dbh, $src_schema, $tgt_schema, $table, $tgt_table) = @_;

    my ($total) = $src_dbh->selectrow_array(
        qq{SELECT COUNT(*) FROM $src_schema.$table}
    );
    log_info("Copying $total row(s) from '$src_schema.$table' "
           . "to '$tgt_schema.$tgt_table' in batches of $batch_size ...");

    return if $total == 0;

    # ── Fetch column names and types in ordinal order ─────────────────────────
    # timestamp columns are EXCLUDED: Sybase manages them automatically as a
    # binary row-version counter.  A user cannot insert into a timestamp column
    # ("a non-null value cannot be inserted into a timestamp column by the user").
    # We cannot use ? placeholders with Sybase/FreeTDS for a prepared INSERT
    # because the server cannot resolve the parameter marker types at prepare
    # time. Instead we build a complete literal INSERT string per row, with
    # every value formatted according to its Sybase data type.
    my $col_sth = $src_dbh->prepare(qq{
        SELECT c.name      AS column_name,
               t.name      AS type_name
        FROM   $src{dbname}..syscolumns c
        JOIN   $src{dbname}..systypes   t ON c.usertype = t.usertype
        WHERE  c.id = OBJECT_ID('$src_schema.$table')
          AND  t.name <> 'timestamp'          -- exclude timestamp: server-managed
        ORDER  BY c.colid
    });
    $col_sth->execute();

    my (@all_cols, @col_types);
    while (my $r = $col_sth->fetchrow_hashref()) {
        push @all_cols,  $r->{column_name};
        push @col_types, lc($r->{type_name});
    }

    if (@all_cols < 1) {
        die "[ERROR] No copyable columns found in '$src_schema.$table'.\n";
    }

    # Log any timestamp columns that were skipped
    my ($ts_count) = $src_dbh->selectrow_array(qq{
        SELECT count(*)
        FROM   $src{dbname}..syscolumns c
        JOIN   $src{dbname}..systypes   t ON c.usertype = t.usertype
        WHERE  c.id    = OBJECT_ID('$src_schema.$table')
          AND  t.name  = 'timestamp'
    });
    log_info("Skipping $ts_count timestamp column(s) – Sybase assigns these automatically.")
        if $ts_count;

    # Detect identity columns – we need SET IDENTITY_INSERT ON for those
    my ($has_identity) = $src_dbh->selectrow_array(qq{
        SELECT count(*)
        FROM   $src{dbname}..syscolumns
        WHERE  id     = OBJECT_ID('$src_schema.$table')
          AND  status & 16 = 16
    });

    my $col_list   = join(', ', @all_cols);
    my $select_sql = "SELECT $col_list FROM $src_schema.$table";

    log_debug("SELECT: $select_sql");

    # Allow inserting into identity columns on the target
    if ($has_identity) {
        log_info("Identity column detected – enabling IDENTITY_INSERT on target ...");
        eval { $tgt_dbh->do("SET IDENTITY_INSERT $tgt_schema.$tgt_table ON") };
        log_warn("Could not set IDENTITY_INSERT: $@") if $@;
    }

    my $insert_prefix = "INSERT INTO $tgt_schema.$tgt_table ($col_list) VALUES ";

    my $sel_sth = $src_dbh->prepare($select_sql);
    $sel_sth->execute();

    my $inserted = 0;
    my $skipped  = 0;
    my @batch;

    while (my $row = $sel_sth->fetchrow_arrayref()) {
        push @batch, [@$row];
        if (@batch >= $batch_size) {
            flush_batch($tgt_dbh, $insert_prefix, \@batch, \@col_types,
                        \$inserted, \$skipped);
            @batch = ();
            log_info(sprintf("  Progress: %d / %d rows (%.1f%%)",
                $inserted, $total, ($inserted / $total) * 100));
        }
    }
    flush_batch($tgt_dbh, $insert_prefix, \@batch, \@col_types,
                \$inserted, \$skipped) if @batch;

    # Turn identity insert off again
    if ($has_identity) {
        eval { $tgt_dbh->do("SET IDENTITY_INSERT $tgt_schema.$tgt_table OFF") };
    }

    log_info("Data copy complete: $inserted row(s) inserted, $skipped skipped (duplicates/errors).");
}

# ─────────────────────────────────────────────
#  Quote a single value for inline Sybase SQL
# ─────────────────────────────────────────────
sub quote_value {
    my ($val, $type) = @_;

    # NULL is always NULL regardless of type
    return 'NULL' unless defined $val;

    # ── Numeric types – no quoting ────────────────────────────────────────────
    if ($type =~ /^(tinyint|smallint|int|bigint|float|real|double|
                     numeric|decimal|money|smallmoney|bit)$/x) {
        # Sanitise: keep only digits, sign, dot, exponent
        (my $safe = $val) =~ s/[^0-9+\-\.eE]//g;
        return $safe;
    }

    # ── Binary / image – hex literal ─────────────────────────────────────────
    if ($type =~ /^(binary|varbinary|image)$/) {
        return '0x' . unpack('H*', $val);
    }

    # ── Timestamp – stored as binary in Sybase ────────────────────────────────
    if ($type eq 'timestamp') {
        return '0x' . unpack('H*', $val);
    }

    # ── All other types (varchar, char, text, datetime, etc.) – quoted string ─
    # Escape any single quotes inside the value by doubling them
    $val =~ s/'/''/g;
    return "'$val'";
}

sub flush_batch {
    my ($tgt_dbh, $insert_prefix, $batch, $col_types, $inserted_ref, $skipped_ref) = @_;
    for my $row (@$batch) {
        my $values = join(', ', map {
            quote_value($row->[$_], $col_types->[$_])
        } 0 .. $#$row);

        my $sql = $insert_prefix . "($values)";
        eval { $tgt_dbh->do($sql) };
        if ($@) {
            $$skipped_ref++;
            log_warn("Row skipped: $@");
            log_debug("Failed SQL: $sql");
        } else {
            $$inserted_ref++;
        }
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
Usage: perl copy_2s2_table.pl [OPTIONS]

Copies a table from one Sybase ASE database to another Sybase ASE database.
Schema structure (columns, identity, defaults, primary key, indexes) is
reproduced using native Sybase DDL syntax.

Required:
  --table=NAME        Source table name
  --src-db=NAME       Source Sybase database name
  --tgt-db=NAME       Target Sybase database name

Source – Sybase ASE (defaults: localhost:5000, user=sa):
  --src-host=HOST
  --src-port=PORT
  --src-user=USER
  --src-pass=PASS
  --src-schema=NAME   Source owner/schema (default: dbo)

Target – Sybase ASE (defaults: localhost:5000, user=sa):
  --tgt-host=HOST
  --tgt-port=PORT
  --tgt-user=USER
  --tgt-pass=PASS
  --tgt-schema=NAME   Target owner/schema (default: dbo)
  --tgt-table=NAME    Target table name   (default: same as --table)

Options:
  --batch=N           Insert batch size (default: 1000)
  --drop              DROP the target table before creating it
  --data-only         Copy data only (skip CREATE TABLE / indexes)
  --struct-only       Copy structure and indexes only (no data)
  --help              Show this help

What is preserved:
  Column names and order      Native Sybase types (varchar, int, money, etc.)
  Length / precision / scale  IDENTITY with original seed and increment
  NULL / NOT NULL             DEFAULT values
  PRIMARY KEY                 CLUSTERED / NONCLUSTERED / UNIQUE indexes

What is NOT copied:
  Foreign key constraints     Triggers
  Stored procedures           Rules and user-defined defaults (bound objects)
  Permissions / grants

Examples:
  # Full copy on the same server, different database
  perl copy_2s2_table.pl \
    --src-db=PROD --tgt-db=STAGING --table=orders

  # Different servers, different table name
  perl copy_2s2_table.pl \
    --src-host=ase1 --src-db=PROD   --src-user=sa --src-pass=secret \
    --tgt-host=ase2 --tgt-db=ARCHIV --tgt-user=sa --tgt-pass=secret \
    --table=orders --tgt-table=orders_2024

  # Structure only – inspect DDL before copying data
  perl copy_2s2_table.pl \
    --src-db=PROD --tgt-db=STAGING --table=orders --struct-only

  # Re-create from scratch and copy data
  perl copy_2s2_table.pl \
    --src-db=PROD --tgt-db=STAGING --table=orders --drop

  # Data only – target table already exists
  perl copy_2s2_table.pl \
    --src-db=PROD --tgt-db=STAGING --table=orders --data-only --batch=5000

  # Different schema/owner on target
  perl copy_2s2_table.pl \
    --src-db=PROD --src-schema=sales \
    --tgt-db=STAGING --tgt-schema=dbo \
    --table=customers --tgt-table=customers_staging

END
    exit 0;
}
