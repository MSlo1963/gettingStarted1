#!/usr/bin/perl
use strict;
use warnings;
use DBI;

# Sample Perl script with embedded SQL commands for testing the SQL extractor

my $dbh = DBI->connect("DBI:mysql:database=testdb", "user", "password");

# Basic SELECT query
my $select_users = "
    SELECT id, name, email
    FROM users
    WHERE active = 1;";
my $sth = $dbh->prepare($select_users);
$sth->execute();

# INSERT statement
my $insert_query = "INSERT INTO users (name, email, created_at) VALUES ('John Doe', 'john@example.com', NOW());";
$dbh->do($insert_query);

# UPDATE with WHERE clause
my $update_sql = "UPDATE users SET last_login = NOW() WHERE id = ?";
my $update_sth = $dbh->prepare($update_sql);
$update_sth->execute(123);

# Complex SELECT with JOINs
my $complex_select = q{
    SELECT u.id, u.name, p.title, c.name as category
    FROM users u
    LEFT JOIN posts p ON u.id = p.user_id
    LEFT JOIN categories c ON p.category_id = c.id
    WHERE u.active = 1
    AND p.published = 1
    ORDER BY p.created_at DESC
    LIMIT 10;
};

# DELETE statement
DELETE FROM sessions WHERE expires_at < NOW();

# CREATE TABLE statement
CREATE TABLE IF NOT EXISTS audit_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    action VARCHAR(50) NOT NULL,
    table_name VARCHAR(50) NOT NULL,
    record_id INT,
    old_values JSON,
    new_values JSON,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(id)
);

# DROP and ALTER statements
DROP TABLE IF EXISTS temp_migration_table;

ALTER TABLE users ADD COLUMN phone VARCHAR(20) AFTER email;

# Transaction statements
BEGIN TRANSACTION;

my $transfer_from = "UPDATE accounts SET balance = balance - 100 WHERE id = 1";
my $transfer_to = "UPDATE accounts SET balance = balance + 100 WHERE id = 2";

$dbh->do($transfer_from);
$dbh->do($transfer_to);

COMMIT;

# Error handling with ROLLBACK
eval {
    $dbh->begin_work();

    # Some risky operations
    my $risky_query = "INSERT INTO critical_data (value) VALUES ('important')";
    $dbh->do($risky_query);

    $dbh->commit();
};
if ($@) {
    warn "Transaction failed: $@";
    ROLLBACK;
}

# GRANT and REVOKE statements
GRANT SELECT, INSERT, UPDATE ON database.users TO 'app_user'@'localhost';
REVOKE DELETE ON database.users FROM 'app_user'@'localhost';

# TRUNCATE statement
TRUNCATE TABLE log_entries;

# String with SQL inside (should also be detected)
my $dynamic_sql = "SELECT * FROM products WHERE category = '$category' AND price < $max_price";

# Another quoted SQL
print "Executing: SELECT COUNT(*) FROM orders WHERE status = 'completed'";

# SQL in different quote styles
my $backup_query = `mysqldump --single-transaction database_name > backup.sql`;

# INSERT with SELECT statement
my $insert_select_query = "
    INSERT INTO user_backup (id, name, email, created_at)
    SELECT id, name, email, NOW()
    FROM users
    WHERE active = 1 AND last_login > DATE_SUB(NOW(), INTERVAL 30 DAY);";
my $insert_select_sth = $dbh->prepare($insert_select_query);
$insert_select_sth->execute();

$dbh->disconnect();

print "Database operations completed.\n";
