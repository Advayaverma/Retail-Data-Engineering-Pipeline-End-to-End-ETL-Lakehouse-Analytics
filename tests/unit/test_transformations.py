"""
Unit tests validating Phase 6 Silver and Gold Data Transformations and Business Metric Calculations.
"""

import os
import csv
import unittest
from src.transformation.cleaner import SilverTransformer, GoldTransformer

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")


class TestTransformations(unittest.TestCase):
    """Test suite validating cleansing, financial calculations, and dimensional joins."""

    def setUp(self):
        self.silver_transformer = SilverTransformer()
        self.gold_transformer = GoldTransformer()

    def test_silver_cleaning_and_metric_derivation(self):
        """Verify SilverTransformer computes gross, discount, and net amounts accurately."""
        sample_bronze = [
            {
                "transaction_id": "TXN-001",
                "customer_id": "CUST-001",
                "product_id": "PRD-001",
                "store_id": "STR-001",
                "transaction_timestamp": "2025-05-10 14:00:00",
                "quantity": "2",
                "unit_price": "50.00",
                "discount": "0.10",
                "payment_method": "credit_card",
            },
            {
                # Duplicate ID -> should be filtered
                "transaction_id": "TXN-001",
                "customer_id": "CUST-001",
                "product_id": "PRD-001",
                "store_id": "STR-001",
                "transaction_timestamp": "2025-05-10 14:00:00",
                "quantity": "2",
                "unit_price": "50.00",
                "discount": "0.10",
                "payment_method": "credit_card",
            },
            {
                # Invalid negative price -> should be filtered
                "transaction_id": "TXN-002",
                "customer_id": "CUST-002",
                "product_id": "PRD-002",
                "store_id": "STR-001",
                "transaction_timestamp": "2025-05-10 14:00:00",
                "quantity": "1",
                "unit_price": "-10.00",
                "discount": "0.0",
                "payment_method": "cash",
            }
        ]
        clean_rows = self.silver_transformer.clean_transactions(sample_bronze)
        self.assertEqual(len(clean_rows), 1)

        row = clean_rows[0]
        self.assertEqual(row["transaction_id"], "TXN-001")
        self.assertEqual(row["gross_amount"], 100.00)
        self.assertEqual(row["discount_amount"], 10.00)
        self.assertEqual(row["net_amount"], 90.00)
        self.assertEqual(row["payment_method"], "Credit Card")
        self.assertEqual(row["txn_year"], 2025)
        self.assertEqual(row["txn_month"], 5)

    def test_gold_fact_sales_assembly_and_profit(self):
        """Verify GoldTransformer enriches dimensions and calculates cost, profit, and margin."""
        sample_silver_txns = [{
            "transaction_id": "TXN-001",
            "customer_id": "CUST-001",
            "product_id": "PRD-001",
            "store_id": "STR-001",
            "quantity": 2,
            "unit_price": 50.00,
            "discount": 0.0,
            "gross_amount": 100.00,
            "discount_amount": 0.0,
            "net_amount": 100.00,
        }]
        sample_products = [{
            "product_id": "PRD-001",
            "product_name": "Test SKU",
            "category": "Electronics",
            "subcategory": "Audio",
            "brand": "TestBrand",
            "unit_cost": "30.00"
        }]
        sample_stores = [{
            "store_id": "STR-001",
            "store_name": "Test Store",
            "city": "Austin",
            "state": "TX",
            "region": "South"
        }]
        sample_customers = [{
            "customer_id": "CUST-001",
            "customer_name": "Alice Smith",
            "customer_segment": "VIP"
        }]

        facts = self.gold_transformer.build_fact_sales(
            sample_silver_txns, sample_products, sample_stores, sample_customers
        )
        self.assertEqual(len(facts), 1)
        f = facts[0]
        self.assertEqual(f["total_cost"], 60.00)  # 2 * 30.00
        self.assertEqual(f["profit"], 40.00)      # 100.00 - 60.00
        self.assertEqual(f["profit_margin_pct"], 40.00)  # (40.00 / 100.00) * 100
        self.assertEqual(f["category"], "Electronics")
        self.assertEqual(f["region"], "South")
        self.assertEqual(f["customer_segment"], "VIP")

    def test_persisted_silver_and_gold_tables(self):
        """Verify Silver and Gold datasets exist on disk and meet volume requirements."""
        silver_txn_path = os.path.join(SILVER_DIR, "transactions", "transactions_silver.csv")
        gold_fact_path = os.path.join(GOLD_DIR, "fact_sales", "fact_sales_gold.csv")
        gold_cat_path = os.path.join(GOLD_DIR, "monthly_category_summary.csv")
        gold_store_path = os.path.join(GOLD_DIR, "store_performance_summary.csv")

        self.assertTrue(os.path.isfile(silver_txn_path), f"Silver transactions missing: {silver_txn_path}")
        self.assertTrue(os.path.isfile(gold_fact_path), f"Gold fact sales missing: {gold_fact_path}")
        self.assertTrue(os.path.isfile(gold_cat_path), f"Gold monthly categories missing: {gold_cat_path}")
        self.assertTrue(os.path.isfile(gold_store_path), f"Gold store summaries missing: {gold_store_path}")

        # Check line count
        with open(gold_fact_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fact_rows = list(reader)
        self.assertGreaterEqual(len(fact_rows), 100000, "Gold Fact Sales should exceed 100k records")


if __name__ == "__main__":
    unittest.main()
