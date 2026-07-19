#!/usr/bin/env perl
#
# run_query.pl
#
# Usage:
#   perl run_query.pl --file profile.yaml --db Syb|Pg --id <n> [--rollback]
#
# Looks up the SQL statement with the given id in the YAML profile file
# (expects a list of entries like: { id => 1, sql => "SELECT ..." }),
# runs it against the chosen database, and reports:
#   - SELECT statements: MD5 hash of normalized output + first 3 rows
#   - INSERT/UPDATE/DELETE: number of affected rows
#
# --rollback: for DML statements, roll back instead of committing
# (has no effect on SELECT, which never commits anything anyway).
#
# DB connection info is read from environment variables:
#   SYB_DSN, SYB_USER, SYB_PASS
#   PG_DSN,  PG_USER,  PG_PASS

use strict;
use warnings;
use Getopt::Long;
use YAML::XS qw(LoadFile);
use DBI;
use Digest::MD5 qw(md5_hex);

my ($file, $db, $id, $rollback);

GetOptions(
    "file=s"   => \$file,
    "db=s"     => \$db,
    "id=i"     => \$id,
    "rollback" => \$rollback,
) or die usage();

die usage() unless $file && $db && defined $id;
die "Error: --db must be 'Syb' or 'Pg'\n" unless $db =~ /^(Syb|Pg)$/i;
die "Error: file '$file' not found\n" unless -e $file;

sub usage {
    return "Usage: $0 --file <profile.yaml> --db Syb|Pg --id <n> [--rollback]\n";
}

# --- Load SQL by ID from the YAML profile file ---
my $entries = LoadFile($file);
my ($entry) = grep { $_->{id} == $id } @$entries;
die "Error: no entry with id=$id found in $file\n" unless $entry;

my $sql = $entry->{sql};
die "Error: entry id=$id has no 'sql' field\n" unless $sql;

print "Running [id=$id] on $db:\n$sql\n\n";

# --- Connect to the chosen DB ---
my $dbh = connect_db($db);

sub connect_db {
    my ($which) = @_;

    if ($which =~ /^Syb$/i) {
        my $dsn  = $ENV{SYB_DSN}  or die "Error: SYB_DSN not set\n";
        my $user = $ENV{SYB_USER} or die "Error: SYB_USER not set\n";
        my $pass = $ENV{SYB_PASS} or die "Error: SYB_PASS not set\n";
        return DBI->connect($dsn, $user, $pass, {
            RaiseError => 1,
            AutoCommit => 0,
        });
    }
    else {
        my $dsn  = $ENV{PG_DSN}  or die "Error: PG_DSN not set\n";
        my $user = $ENV{PG_USER} or die "Error: PG_USER not set\n";
        my $pass = $ENV{PG_PASS} or die "Error: PG_PASS not set\n";
        return DBI->connect($dsn, $user, $pass, {
            RaiseError => 1,
            AutoCommit => 0,
        });
    }
}

# --- Normalize a row string before hashing (mirrors harness normalization) ---
sub normalize_row {
    my ($row) = @_;
    $row =~ s/\s+/ /g;                                  # collapse whitespace
    $row =~ s/(\d+)\.(\d{2})\d*/$1.$2/g;                 # round decimals to 2 places
    $row =~ s{(\d{4})-(\d{2})-(\d{2})}{$1\/$2\/$3}g;      # normalize date separator
    return $row;
}

# --- Determine statement type from the first keyword ---
my ($verb) = $sql =~ /^\s*(\w+)/;
$verb = uc($verb // '');

eval {
    if ($verb eq 'SELECT') {
        run_select($dbh, $sql);
    }
    elsif ($verb =~ /^(INSERT|UPDATE|DELETE)$/) {
        run_dml($dbh, $sql, $rollback);
    }
    else {
        die "Unsupported SQL type: $verb\n";
    }
    1;
} or do {
    my $err = $@;
    $dbh->rollback if $dbh;
    $dbh->disconnect if $dbh;
    die "Error: $err";
};

$dbh->disconnect;
exit 0;

# --- SELECT handling: MD5 + preview ---
sub run_select {
    my ($dbh, $sql) = @_;

    my $sth = $dbh->prepare($sql);
    $sth->execute;
    my $rows = $sth->fetchall_arrayref;

    my @normalized = map {
        normalize_row(join(",", map { defined($_) ? $_ : '' } @$_))
    } @$rows;

    my $hash = md5_hex(join("\n", @normalized));

    print "MD5: $hash\n";
    print "Row count: " . scalar(@$rows) . "\n";
    print "First 3 rows:\n";

    my $preview_count = scalar(@normalized) > 3 ? 3 : scalar(@normalized);
    for my $i (0 .. $preview_count - 1) {
        print "  $normalized[$i]\n";
    }

    # SELECT never modifies data; rollback just closes the transaction cleanly
    $dbh->rollback;
}

# --- INSERT/UPDATE/DELETE handling: affected row count + commit/rollback ---
sub run_dml {
    my ($dbh, $sql, $rollback) = @_;

    my $rows_affected = $dbh->do($sql);
    $rows_affected = 0 if !defined $rows_affected || $rows_affected eq '0E0';

    print "Rows affected: $rows_affected\n";

    if ($rollback) {
        $dbh->rollback;
        print "ROLLBACK applied - changes NOT committed.\n";
    }
    else {
        $dbh->commit;
        print "COMMIT applied - changes saved.\n";
    }
}
