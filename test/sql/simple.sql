-- Simple test cases for pg_orca without external dependencies
-- This test suite only uses PostgreSQL built-in functionality

-- Disable ORCA initially
SET pg_orca.enable_orca TO off;

-- Create simple test tables
CREATE TABLE test_table (
    id int PRIMARY KEY,
    name text,
    value numeric
);

CREATE TABLE test_orders (
    order_id int PRIMARY KEY,
    customer_id int,
    amount numeric,
    order_date date
);

CREATE TABLE test_customers (
    customer_id int PRIMARY KEY,
    customer_name text,
    region text
);

-- Insert some test data
INSERT INTO test_table VALUES (1, 'one', 10.5), (2, 'two', 20.3), (3, 'three', 30.7);
INSERT INTO test_orders VALUES (1, 1, 100.00, '2024-01-01'), (2, 1, 200.00, '2024-01-02'), (3, 2, 150.00, '2024-01-03');
INSERT INTO test_customers VALUES (1, 'Alice', 'North'), (2, 'Bob', 'South'), (3, 'Charlie', 'East');

-- Test basic queries with ORCA disabled
EXPLAIN VERBOSE SELECT * FROM test_table;
EXPLAIN VERBOSE SELECT * FROM test_table WHERE id = 1;

-- Enable ORCA
SET pg_orca.enable_orca TO on;
SET pg_orca.enable_new_planner TO on;

-- Basic SELECT tests
SELECT * FROM test_table ORDER BY id;
EXPLAIN VERBOSE SELECT * FROM test_table;
EXPLAIN VERBOSE SELECT * FROM test_table WHERE id > 1;
EXPLAIN VERBOSE SELECT id, name FROM test_table;

-- Test VALUES
VALUES(1,2);
VALUES(1,2),(3,4);

-- Test constant expressions
EXPLAIN VERBOSE SELECT 1 + 1;
SELECT 1, '1', 2 AS x, 'xx' AS y;
SELECT 1, 1 + 1, true, null, array[1, 2, 3];

-- Test type casts
SELECT 1::text;
SELECT '{1,2,3}'::integer[], 1::text, 1::int, 'a'::text;

-- Test WHERE clause with expressions
EXPLAIN VERBOSE SELECT * FROM test_table WHERE id = 1;
EXPLAIN VERBOSE SELECT * FROM test_table WHERE value > 15.0;
EXPLAIN VERBOSE SELECT * FROM test_table WHERE 1 IN (2, 3);

-- Test with generate_series
EXPLAIN VERBOSE SELECT 1 FROM generate_series(1,10);
EXPLAIN VERBOSE SELECT g FROM generate_series(1,10) g;
EXPLAIN VERBOSE SELECT g + 1 FROM generate_series(1,10) g;
EXPLAIN VERBOSE SELECT g + 1 AS x FROM generate_series(1,10) g WHERE 1 < 10;

-- Test LIMIT and ORDER BY
EXPLAIN VERBOSE SELECT * FROM test_table ORDER BY id LIMIT 2;
EXPLAIN VERBOSE SELECT * FROM test_table ORDER BY value DESC LIMIT 2;

-- Test arithmetic operations
EXPLAIN VERBOSE SELECT value + 1, value - 1, value * 2, value / 2 FROM test_table WHERE id = 1;

-- Test aggregates
EXPLAIN VERBOSE SELECT sum(value) FROM test_table;
EXPLAIN VERBOSE SELECT count(*) FROM test_table;
EXPLAIN VERBOSE SELECT avg(value) FROM test_table;

-- Test GROUP BY
EXPLAIN VERBOSE SELECT name, sum(value) FROM test_table GROUP BY name;
EXPLAIN VERBOSE SELECT id, count(*) FROM test_table GROUP BY id ORDER BY id;

-- Test UNION operations
EXPLAIN VERBOSE SELECT id FROM test_table WHERE id < 2 UNION SELECT id FROM test_table WHERE id > 2 ORDER BY id;
EXPLAIN VERBOSE SELECT id FROM test_table WHERE id < 2 UNION ALL SELECT id FROM test_table WHERE id > 2 ORDER BY id;

-- Test JOIN operations
EXPLAIN SELECT * FROM test_orders JOIN test_customers ON test_orders.customer_id = test_customers.customer_id;
EXPLAIN SELECT * FROM test_orders o JOIN test_customers c ON o.customer_id = c.customer_id WHERE c.region = 'North';

-- Test LEFT JOIN
EXPLAIN SELECT * FROM test_customers LEFT JOIN test_orders ON test_customers.customer_id = test_orders.customer_id;
EXPLAIN SELECT * FROM test_customers c LEFT JOIN test_orders o ON c.customer_id = o.customer_id WHERE c.region = 'North';

-- Test EXCEPT and INTERSECT
EXPLAIN SELECT id FROM test_table EXCEPT SELECT id FROM test_table WHERE id = 2;
EXPLAIN SELECT id FROM test_table INTERSECT SELECT id FROM test_table WHERE id > 0;

-- Test subqueries
EXPLAIN VERBOSE SELECT * FROM test_customers WHERE customer_id IN (SELECT customer_id FROM test_orders);
EXPLAIN VERBOSE SELECT * FROM test_customers WHERE customer_id IN (SELECT customer_id FROM test_orders WHERE amount > 100);

-- Test EXISTS
EXPLAIN VERBOSE SELECT * FROM test_customers WHERE EXISTS (SELECT 1 FROM test_orders WHERE test_orders.customer_id = test_customers.customer_id);

-- Test scalar subquery
EXPLAIN VERBOSE SELECT * FROM test_customers WHERE customer_id > (SELECT avg(customer_id) FROM test_orders);

-- Test CTE (Common Table Expressions)
EXPLAIN VERBOSE WITH cte AS (SELECT * FROM test_orders) SELECT * FROM cte;
EXPLAIN VERBOSE WITH cte AS (SELECT customer_id, sum(amount) AS total FROM test_orders GROUP BY customer_id) SELECT * FROM cte WHERE total > 100;

-- Test nested subqueries
EXPLAIN VERBOSE SELECT * FROM test_customers WHERE customer_id IN (
    SELECT customer_id FROM test_orders WHERE order_id IN (
        SELECT order_id FROM test_orders WHERE amount > 100
    )
);

-- Test complex joins
EXPLAIN VERBOSE SELECT c.customer_name, o.order_id, o.amount
FROM test_customers c
JOIN test_orders o ON c.customer_id = o.customer_id
WHERE c.region IN ('North', 'South')
ORDER BY o.amount DESC;

-- Test self-join
EXPLAIN VERBOSE SELECT a.customer_id, b.customer_id
FROM test_customers a
JOIN test_customers b ON a.customer_id != b.customer_id;

-- Execute some queries to verify correctness
SELECT count(*) FROM test_table;
SELECT sum(value) FROM test_table;
SELECT * FROM test_customers WHERE customer_id IN (SELECT customer_id FROM test_orders);
SELECT c.customer_name, count(o.order_id) AS order_count
FROM test_customers c
LEFT JOIN test_orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_name
ORDER BY c.customer_name;

-- Disable new planner
SET pg_orca.enable_new_planner TO off;

-- Additional tests with new planner off
EXPLAIN VERBOSE SELECT * FROM test_customers WHERE EXISTS (
    SELECT * FROM test_orders
    WHERE test_orders.customer_id = test_customers.customer_id
    AND test_orders.amount > 100
);

EXPLAIN VERBOSE SELECT * FROM test_customers WHERE customer_id IN (
    SELECT DISTINCT customer_id FROM test_orders
);

-- Cleanup
DROP TABLE test_table CASCADE;
DROP TABLE test_orders CASCADE;
DROP TABLE test_customers CASCADE;
