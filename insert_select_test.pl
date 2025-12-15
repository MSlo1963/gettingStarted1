#!/usr/bin/perl
use strict;
use warnings;

# Simple test file for INSERT with SELECT detection

# Single line INSERT with SELECT
my $simple_insert_select = "INSERT INTO backup_users SELECT * FROM users WHERE active = 1;";

# Multi-line INSERT with SELECT (the problematic one)
my $insert_select_query = "
    INSERT INTO user_backup (id, name, email, created_at)
    SELECT id, name, email, NOW()
    FROM users
    WHERE active = 1 AND last_login > DATE_SUB(NOW(), INTERVAL 30 DAY);";

# Another multi-line version with different formatting
my $complex_insert_select = "INSERT INTO archive_orders (order_id, customer_id, total, archived_date)
SELECT o.id, o.customer_id, o.total, NOW()
FROM orders o
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'completed'
AND o.created_at < DATE_SUB(NOW(), INTERVAL 1 YEAR);";

# INSERT with VALUES (for comparison)
my $regular_insert = "INSERT INTO users (name, email) VALUES ('Test User', 'test@example.com');";

# INSERT with SELECT using different quote styles
my $quoted_insert_select = 'INSERT INTO daily_summary (date, total_orders, revenue)
SELECT DATE(created_at), COUNT(*), SUM(total)
FROM orders
WHERE DATE(created_at) = CURDATE()
GROUP BY DATE(created_at);';

print "Test file for INSERT with SELECT statements\n";
