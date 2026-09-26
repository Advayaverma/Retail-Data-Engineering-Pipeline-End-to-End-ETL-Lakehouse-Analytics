"""
Unit tests validating Phase 8 Kimball Star Schema and Slowly Changing Dimension Type 2 (SCD Type 2).
"""

import os
import csv
import unittest
from src.modelling.date_dimension import generate_date_dimension
from src.modelling.scd2_handler import SCD2CustomerHandler

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")


class TestStarSchemaAndSCD2(unittest.TestCase):
    """Test suite validating star schema dimensions, facts, surrogate keys, and SCD Type 2."""

    def test_date_dimension_generation(self):
        """Verify dim_date attributes, fiscal calendar, and surrogate keys."""
        dates = generate_date_dimension("2024-01-01", "2024-01-10")
        self.assertEqual(len(dates), 10)
        d0 = dates[0]
        self.assertEqual(d0["date_sk"], 20240101)
        self.assertEqual(d0["day_name"], "Monday")
        self.assertFalse(d0["is_weekend"])
        self.assertEqual(d0["quarter_name"], "Q1")
        self.assertIn("FQ", d0["fiscal_quarter"])

    def test_scd2_lifecycle(self):
        """Verify SCD Type 2 expires old records and inserts new versions with new SK."""
        baseline = [
            {"customer_id": "C-1", "customer_name": "Alice", "city": "Dallas", "state": "TX", "customer_segment": "Standard", "signup_date": "2022-01-01"},
            {"customer_id": "C-2", "customer_name": "Bob", "city": "Chicago", "state": "IL", "customer_segment": "Standard", "signup_date": "2022-01-01"},
        ]
        handler = SCD2CustomerHandler()
        dim_cust = handler.initialize_dim_customer(baseline)
        self.assertEqual(len(dim_cust), 2)
        self.assertTrue(dim_cust[0]["is_current"])
        self.assertEqual(dim_cust[0]["effective_end_date"], "9999-12-31")

        # Update Alice's segment to VIP and city to New York
        updates = [
            {"customer_id": "C-1", "customer_name": "Alice", "city": "New York", "state": "NY", "customer_segment": "VIP", "effective_update_date": "2025-06-01"}
        ]
        dim_cust_v2, expired, new_vers = handler.process_scd2_updates(dim_cust, updates)
        self.assertEqual(expired, 1)
        self.assertEqual(new_vers, 1)
        self.assertEqual(len(dim_cust_v2), 3)  # 1 expired + 2 current

        # Verify expired version
        alice_expired = next(r for r in dim_cust_v2 if r["customer_id"] == "C-1" and not r["is_current"])
        self.assertEqual(alice_expired["city"], "Dallas")
        self.assertEqual(alice_expired["effective_end_date"], "2025-06-01")

        # Verify current active version
        alice_current = next(r for r in dim_cust_v2 if r["customer_id"] == "C-1" and r["is_current"])
        self.assertEqual(alice_current["city"], "New York")
        self.assertEqual(alice_current["customer_segment"], "VIP")
        self.assertEqual(alice_current["effective_start_date"], "2025-06-01")
        self.assertEqual(alice_current["effective_end_date"], "9999-12-31")
        self.assertNotEqual(alice_expired["customer_sk"], alice_current["customer_sk"])

    def test_gold_star_schema_files_exist_and_valid(self):
        """Verify presence of Gold star schema tables and measure arithmetic."""
        fact_path = os.path.join(GOLD_DIR, "fact_sales", "fact_sales.csv")
        cust_path = os.path.join(GOLD_DIR, "dim_customer", "dim_customer.csv")
        date_path = os.path.join(GOLD_DIR, "dim_date", "dim_date.csv")
        prod_path = os.path.join(GOLD_DIR, "dim_product", "dim_product.csv")
        store_path = os.path.join(GOLD_DIR, "dim_store", "dim_store.csv")

        for p in [fact_path, cust_path, date_path, prod_path, store_path]:
            self.assertTrue(os.path.isfile(p), f"Gold table missing: {p}")

        # Verify fact_sales measures
        with open(fact_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            sample_rows = [next(reader) for _ in range(20)]

        for r in sample_rows:
            qty = int(r["quantity"])
            price = float(r["unit_price"])
            gross = float(r["gross_sales"])
            disc_amt = float(r["discount_amount"])
            net = float(r["net_sales"])
            cost = float(r["cost_amount"])
            profit = float(r["profit"])

            self.assertAlmostEqual(gross, round(qty * price, 2), places=1)
            self.assertAlmostEqual(net, round(gross - disc_amt, 2), places=1)
            self.assertAlmostEqual(profit, round(net - cost, 2), places=1)
            self.assertGreater(int(r["date_sk"]), 20200000)
            self.assertGreater(int(r["customer_sk"]), 0)
            self.assertGreater(int(r["product_sk"]), 0)
            self.assertGreater(int(r["store_sk"]), 0)


if __name__ == "__main__":
    unittest.main()
