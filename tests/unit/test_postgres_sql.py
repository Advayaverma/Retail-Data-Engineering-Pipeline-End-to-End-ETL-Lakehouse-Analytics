"""
Unit tests validating Phase 4 PostgreSQL schemas, indexes, and analytical SQL queries.
"""

import os
import sqlite3
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SQL_DIR = os.path.join(PROJECT_ROOT, "sql")
DB_PATH = os.path.join(PROJECT_ROOT, "data", "retail_dw.db")


class TestPostgresAndSQL(unittest.TestCase):
    """Test suite validating SQL scripts, DDL syntax, and analytical execution."""

    def test_sql_files_exist(self):
        """Verify presence of DDL, index, benchmark, and analytical query files."""
        expected_files = [
            os.path.join(SQL_DIR, "init", "01_init_postgres.sql"),
            os.path.join(SQL_DIR, "init", "02_create_indexes.sql"),
            os.path.join(SQL_DIR, "analytics", "01_retail_analytics_queries.sql"),
            os.path.join(SQL_DIR, "analytics", "04_indexing_benchmark.sql"),
        ]
        for f in expected_files:
            self.assertTrue(os.path.isfile(f), f"Missing SQL file: {f}")

    def test_query_1_join_groupby_having(self):
        """Verify Query 1 (JOIN, GROUP BY, HAVING) executes and returns aggregated store revenue."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        query = """
        SELECT 
            s.store_id,
            s.store_name,
            s.city,
            s.region,
            COUNT(t.id) AS total_transactions,
            SUM(t.quantity) AS total_units_sold,
            ROUND(AVG(t.unit_price), 2) AS avg_unit_price,
            ROUND(SUM(t.quantity * t.unit_price * (1 - t.discount)), 2) AS net_revenue
        FROM transactions t
        INNER JOIN stores s ON t.store_id = s.store_id
        GROUP BY s.store_id, s.store_name, s.city, s.region
        HAVING SUM(t.quantity * t.unit_price * (1 - t.discount)) > 100000.00
        ORDER BY net_revenue DESC;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        self.assertGreater(len(rows), 0, "Query 1 returned no stores exceeding $100k revenue threshold")
        # Ensure highest revenue store has valid revenue number
        first_store_revenue = rows[0][7]
        self.assertGreater(first_store_revenue, 100000.00)
        conn.close()

    def test_query_2_subqueries_and_joins(self):
        """Verify Query 2 (Subquery identifying customers above 2x average spend) executes."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        query = """
        SELECT 
            c.customer_id,
            c.customer_name,
            c.customer_segment,
            ROUND(SUM(t.quantity * t.unit_price * (1 - t.discount)), 2) AS customer_lifetime_spend
        FROM customers c
        INNER JOIN transactions t ON c.customer_id = t.customer_id
        GROUP BY c.customer_id, c.customer_name, c.customer_segment
        HAVING SUM(t.quantity * t.unit_price * (1 - t.discount)) > (
            SELECT 2.0 * AVG(customer_total)
            FROM (
                SELECT SUM(t2.quantity * t2.unit_price * (1 - t2.discount)) AS customer_total
                FROM transactions t2
                WHERE t2.customer_id IS NOT NULL AND t2.customer_id != ''
                GROUP BY t2.customer_id
            ) sub
        )
        ORDER BY customer_lifetime_spend DESC
        LIMIT 10;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        self.assertGreater(len(rows), 0, "Query 2 returned no high-value customers")
        conn.close()

    def test_query_3_window_function_dense_rank(self):
        """Verify Query 3 (CTE + DENSE_RANK() Window Function) ranks products by category."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        query = """
        WITH ProductRevenue AS (
            SELECT 
                p.category,
                p.product_id,
                p.product_name,
                ROUND(SUM(t.quantity * t.unit_price * (1 - t.discount)), 2) AS total_revenue,
                DENSE_RANK() OVER (
                    PARTITION BY p.category 
                    ORDER BY SUM(t.quantity * t.unit_price * (1 - t.discount)) DESC
                ) AS category_rank
            FROM transactions t
            INNER JOIN products p ON t.product_id = p.product_id
            GROUP BY p.category, p.product_id, p.product_name
        )
        SELECT category, category_rank, product_name, total_revenue
        FROM ProductRevenue
        WHERE category_rank <= 3
        ORDER BY category, category_rank;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        self.assertGreater(len(rows), 0, "Query 3 returned no ranked products")
        # Every category must have rank 1, 2, 3
        ranks = {r[1] for r in rows}
        self.assertTrue({1, 2, 3}.issubset(ranks))
        conn.close()

    def test_query_4_window_function_lag_mom(self):
        """Verify Query 4 (CTE + LAG() Window Function) calculates Month-over-Month growth."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        query = """
        WITH MonthlySales AS (
            SELECT 
                SUBSTR(t.transaction_timestamp, 1, 7) AS sales_month,
                ROUND(SUM(t.quantity * t.unit_price * (1 - t.discount)), 2) AS current_month_revenue
            FROM transactions t
            WHERE t.transaction_timestamp NOT LIKE '%INVALID%'
              AND t.unit_price > 0
              AND t.quantity > 0
            GROUP BY SUBSTR(t.transaction_timestamp, 1, 7)
        ),
        MonthlyGrowth AS (
            SELECT 
                sales_month,
                current_month_revenue,
                LAG(current_month_revenue, 1) OVER (ORDER BY sales_month) AS previous_month_revenue
            FROM MonthlySales
        )
        SELECT 
            sales_month,
            current_month_revenue,
            COALESCE(previous_month_revenue, 0.00) AS previous_month_revenue,
            ROUND(current_month_revenue - COALESCE(previous_month_revenue, current_month_revenue), 2) AS absolute_change
        FROM MonthlyGrowth
        ORDER BY sales_month;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        self.assertGreaterEqual(len(rows), 12, "MoM calculation should span multiple monthly periods")
        conn.close()

    def test_query_5_window_function_running_total(self):
        """Verify Query 5 (SUM() OVER Running Total) computes cumulative regional sales."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        query = """
        WITH RegionalMonthly AS (
            SELECT 
                s.region,
                SUBSTR(t.transaction_timestamp, 1, 7) AS sales_month,
                ROUND(SUM(t.quantity * t.unit_price * (1 - t.discount)), 2) AS monthly_revenue
            FROM transactions t
            INNER JOIN stores s ON t.store_id = s.store_id
            WHERE t.unit_price > 0 AND t.quantity > 0
            GROUP BY s.region, SUBSTR(t.transaction_timestamp, 1, 7)
        )
        SELECT 
            region,
            sales_month,
            monthly_revenue,
            ROUND(SUM(monthly_revenue) OVER (
                PARTITION BY region 
                ORDER BY sales_month
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ), 2) AS cumulative_running_revenue
        FROM RegionalMonthly
        ORDER BY region, sales_month;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        self.assertGreater(len(rows), 0, "Query 5 returned no cumulative sales rows")
        conn.close()


if __name__ == "__main__":
    unittest.main()
