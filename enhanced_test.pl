#!/usr/bin/perl
use strict;
use warnings;
use DBI;

# Enhanced test file with proper variable names for SQL extractor testing

my $dbh = DBI->connect("DBI:mysql:database=testdb", "user", "password");

# User management queries
my $get_active_users = "SELECT id, name, email, created_at
                       FROM users
                       WHERE active = 1
                       AND deleted_at IS NULL
                       ORDER BY created_at DESC";

my $user_insert_query = "INSERT INTO users (name, email, password_hash, created_at)
                        VALUES (?, ?, ?, NOW())";

my $update_user_status = "UPDATE users
                         SET status = 'active',
                             last_login = NOW()
                         WHERE id = ?
                         AND deleted_at IS NULL";

# Product management
my $product_search = "SELECT p.id, p.name, p.price, c.name as category_name
                     FROM products p
                     LEFT JOIN categories c ON p.category_id = c.id
                     WHERE p.active = 1
                     AND p.price BETWEEN ? AND ?
                     ORDER BY p.name ASC
                     LIMIT 50";

my $delete_expired_products = "DELETE FROM products
                              WHERE expires_at < NOW()
                              AND status = 'expired'";

# Database schema operations
my $create_audit_table = "CREATE TABLE IF NOT EXISTS audit_log (
                         id BIGINT AUTO_INCREMENT PRIMARY KEY,
                         table_name VARCHAR(64) NOT NULL,
                         operation ENUM('INSERT', 'UPDATE', 'DELETE') NOT NULL,
                         record_id BIGINT NOT NULL,
                         old_values JSON,
                         new_values JSON,
                         user_id INT,
                         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                         INDEX idx_table_record (table_name, record_id),
                         INDEX idx_created_at (created_at)
                         )";

my $alter_users_table = "ALTER TABLE users
                        ADD COLUMN phone VARCHAR(20) AFTER email,
                        ADD COLUMN timezone VARCHAR(50) DEFAULT 'UTC'";

# Transaction management
my $begin_transaction = "BEGIN";
my $commit_changes = "COMMIT";
my $rollback_changes = "ROLLBACK";

# Permissions and security
my $grant_select_permissions = "GRANT SELECT, INSERT, UPDATE
                               ON database.users
                               TO 'app_user'@'localhost'
                               IDENTIFIED BY 'secure_password'";

my $revoke_delete_access = "REVOKE DELETE ON database.sensitive_data
                           FROM 'temp_user'@'%'";

# Maintenance operations
my $truncate_logs = "TRUNCATE TABLE access_logs";

my $cleanup_sessions = "DELETE FROM user_sessions
                       WHERE expires_at < NOW()
                       OR last_activity < DATE_SUB(NOW(), INTERVAL 30 DAY)";

# Complex reporting query
my $monthly_report = "SELECT
                         DATE_FORMAT(o.created_at, '%Y-%m') as month,
                         COUNT(o.id) as total_orders,
                         SUM(o.total) as revenue,
                         AVG(o.total) as avg_order_value,
                         COUNT(DISTINCT o.customer_id) as unique_customers
                     FROM orders o
                     WHERE o.created_at >= DATE_SUB(NOW(), INTERVAL 12 MONTH)
                     AND o.status = 'completed'
                     GROUP BY DATE_FORMAT(o.created_at, '%Y-%m')
                     ORDER BY month DESC";

# Backup and restore
my $backup_command = "SELECT * INTO OUTFILE '/backup/users_backup.csv'
                     FIELDS TERMINATED BY ','
                     ENCLOSED BY '\"'
                     LINES TERMINATED BY '\n'
                     FROM users
                     WHERE created_at >= '2024-01-01'";

# Comments with SQL that should be ignored
# This should not be extracted: SELECT * FROM ignore_me WHERE test = 1;
# Another ignored query: UPDATE ignored_table SET value = 'test';

/*
 * Multi-line comment with SQL to ignore:
 * INSERT INTO comment_table (data) VALUES ('ignored');
 * DELETE FROM comment_data WHERE id > 0;
 */

// Also ignore this: CREATE TABLE ignored_table (id INT);

print "Enhanced SQL extraction test completed.\n";

$dbh->disconnect();
