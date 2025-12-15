#!/usr/bin/perl
use strict;
use warnings;
use DBI;

# Test file with SQL patterns in comments to verify comment filtering

my $dbh = DBI->connect("DBI:mysql:database=testdb", "user", "password");

# This is a comment with SQL that should be ignored: SELECT * FROM users WHERE active = 1;
# Another comment: UPDATE users SET status = 'inactive' WHERE id = 5;
# DELETE FROM old_records WHERE created_at < '2020-01-01';

my $valid_sql = "SELECT id, name FROM customers WHERE region = 'US';";

# Multi-line comment example:
# CREATE TABLE test_table (
#     id INT PRIMARY KEY,
#     name VARCHAR(255)
# );

/*
 This is a C-style comment that should be ignored:
 INSERT INTO products (name, price) VALUES ('Widget', 19.99);
 UPDATE inventory SET quantity = 0 WHERE product_id = 123;
*/

my $another_valid_sql = "INSERT INTO orders (customer_id, total) VALUES (?, ?);";

// This is a C++ style comment: DROP TABLE temporary_data;
// SELECT COUNT(*) FROM sessions WHERE expires_at > NOW();

-- This is an SQL-style comment: TRUNCATE TABLE logs;
-- GRANT ALL PRIVILEGES ON database.* TO 'admin'@'localhost';

# Comment before valid SQL
my $update_query = "UPDATE products SET price = price * 1.1 WHERE category = 'electronics';";

/* Another multi-line comment:
   ALTER TABLE users ADD COLUMN phone VARCHAR(20);
   REVOKE SELECT ON sensitive_data FROM 'guest'@'%';
*/

# Valid SQL that should be extracted:
my $delete_query = "DELETE FROM expired_tokens WHERE expires_at < NOW();";

print "Database operations with comments test completed.\n";

# Final comment with SQL: BEGIN TRANSACTION; COMMIT; ROLLBACK;
