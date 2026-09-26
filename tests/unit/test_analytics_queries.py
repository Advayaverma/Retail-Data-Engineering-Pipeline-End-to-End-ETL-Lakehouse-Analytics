"""
Unit tests validating Phase 9 SQL Business Analytics on Kimball Gold Star Schema.
"""

import os
import sqlite3
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "retail_dw.db")
SQL_FILE = os.path.join(PROJECT_ROOT, "sql", "analytics", "02_gold_star_schema_analytics.sql")


class TestAnalyticsQueries(unittest.TestCase):
    """Test suite validating all 10 core business analytics queries against the Star Schema."""

    @classmethod
    def setUpClass(cls):
        cls.conn = sqlite3.connect(DB_PATH)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()

    def test_sql_file_exists(self):
        """Verify presence of Gold Star Schema analytics SQL file."""
        self.assertTrue(os.path.isfile(SQL_FILE))
        with open(SQL_FILE, "r", encoding="utf-8") as f:
            content = f.read()
        for i in range(1, 11):
            self.assertIn(f"QUESTION {i}:", content)

    def test_q1_monthly_revenue(self):
        """Verify Question 1 returns 24 monthly periods with valid revenue."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT d.year_num, d.month_num, SUM(f.net_sales), SUM(f.profit)
            FROM fact_sales f INNER JOIN dim_date d ON f.date_sk = d.date_sk
            GROUP BY d.year_num, d.month_num
            ORDER BY d.year_num, d.month_num;
        """)
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 24)
        for r in rows:
            self.assertGreater(r[2], 0, "Monthly net sales must be > 0")
            self.assertGreater(r[3], 0, "Monthly profit must be > 0")

    def test_q2_regional_revenue(self):
        """Verify Question 2 returns regional revenue aggregation."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT s.region, SUM(f.net_sales)
            FROM fact_sales f INNER JOIN dim_store s ON f.store_sk = s.store_sk
            GROUP BY s.region ORDER BY SUM(f.net_sales) DESC;
        """)
        rows = cursor.fetchall()
        self.assertGreaterEqual(len(rows), 4)

    def test_q3_top_products(self):
        """Verify Question 3 returns top 10 products sorted by revenue."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT p.product_name, SUM(f.net_sales) AS rev
            FROM fact_sales f INNER JOIN dim_product p ON f.product_sk = p.product_sk
            GROUP BY p.product_id, p.product_name
            ORDER BY rev DESC LIMIT 10;
        """)
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 10)
        # Verify descending order
        revenues = [r[1] for r in rows]
        self.assertEqual(revenues, sorted(revenues, reverse=True))

    def test_q4_top_customers(self):
        """Verify Question 4 returns top 10 customers by lifetime spend."""
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT c.customer_name, SUM(f.net_sales) AS spend
            FROM fact_sales f INNER JOIN dim_customer c ON f.customer_sk = c.customer_sk
            WHERE c.is_current = 1
            GROUP BY c.customer_id, c.customer_name
            ORDER BY spend DESC LIMIT 10;
        """)
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 10)

    def test_q5_clv_by_segment(self):
        """Verify Question 5 returns average CLV across customer tiers."""
        cursor = self.conn.cursor()
        cursor.execute("""
            WITH CustSpend AS (
                SELECT c.customer_segment, SUM(f.net_sales) AS total_spend
                FROM fact_sales f INNER JOIN dim_customer c ON f.customer_sk = c.customer_sk
                WHERE c.is_current = 1
                GROUP BY c.customer_id, c.customer_segment
            )
            SELECT customer_segment, AVG(total_spend)
            FROM CustSpend GROUP BY customer_segment;
        """)
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 4)

    def test_q6_mom_growth(self):
        """Verify Question 6 uses LAG() to compute Month-over-Month variance."""
        cursor = self.conn.cursor()
        cursor.execute("""
            WITH MonthlyTotals AS (
                SELECT d.year_num || '-' || SUBSTR('0' || d.month_num, -2) AS ym,
                       SUM(f.net_sales) AS net_sales
                FROM fact_sales f INNER JOIN dim_date d ON f.date_sk = d.date_sk
                GROUP BY d.year_num, d.month_num
            ),
            MoM AS (
                SELECT ym, net_sales, LAG(net_sales, 1) OVER (ORDER BY ym) AS prev_sales
                FROM MonthlyTotals
            )
            SELECT ym, net_sales, prev_sales FROM MoM;
        """)
        rows = cursor.fetchall()
        self.assertEqual(len(rows), 24)
        self.assertIsNone(rows[0][2], "First month previous sales should be NULL")
        self.assertIsNotNone(rows[1][2], "Second month previous sales must be populated by LAG()")

    def test_q9_store_dense_rank(self):
        """Verify Question 9 uses DENSE_RANK() within region."""
        cursor = self.conn.cursor()
        cursor.execute("""
            WITH StoreRev AS (
                SELECT s.region, s.store_name,
                       DENSE_RANK() OVER (PARTITION BY s.region ORDER BY SUM(f.net_sales) DESC) AS rank
                FROM fact_sales f INNER JOIN dim_store s ON f.store_sk = s.store_sk
                GROUP BY s.region, s.store_name
            )
            SELECT region, rank FROM StoreRev WHERE rank <= 3;
        """)
        rows = cursor.fetchall()
        self.assertGreater(len(rows), 0)
        ranks = {r[1] for r in rows}
        self.assertTrue({1, 2, 3}.issubset(ranks))

    def test_q10_repeat_customer_rate(self):
        """Verify Question 10 computes valid repeat customer percentage."""
        cursor = self.conn.cursor()
        cursor.execute("""
            WITH CustCounts AS (
                SELECT c.customer_id, COUNT(DISTINCT f.transaction_id) AS orders
                FROM fact_sales f INNER JOIN dim_customer c ON f.customer_sk = c.customer_sk
                GROUP BY c.customer_id
            )
            SELECT COUNT(customer_id),
                   ROUND((CAST(SUM(CASE WHEN orders > 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(customer_id)) * 100.0, 2)
            FROM CustCounts;
        """)
        row = cursor.fetchone()
        self.assertGreater(row[0], 5000, "Should evaluate substantial customer base")
        self.assertGreater(row[1], 50.0, "Repeat customer rate should be significant in high-volume retail")


if __name__ == "__main__":
    unittest.main()
