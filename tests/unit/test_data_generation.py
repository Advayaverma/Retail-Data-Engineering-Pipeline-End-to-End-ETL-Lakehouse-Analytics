"""Unit tests verifying synthetic data generation results and defect integrity."""
import os
import csv
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")


class TestDataGeneration(unittest.TestCase):
    """Test suite validating Phase 2 synthetic datasets."""

    def test_all_raw_files_exist(self):
        """Check all required raw CSV files are generated."""
        expected_files = [
            "stores.csv",
            "products.csv",
            "customers.csv",
            "transactions.csv",
            "transactions_day2.csv",
            "customers_updates_day2.csv",
        ]
        for fname in expected_files:
            fpath = os.path.join(RAW_DATA_DIR, fname)
            self.assertTrue(os.path.isfile(fpath), f"Missing generated file: {fname}")

    def test_stores_volume_and_schema(self):
        """Verify stores meet volume (>=50) and schema."""
        path = os.path.join(RAW_DATA_DIR, "stores.csv")
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        self.assertGreaterEqual(len(rows), 50)
        expected_fields = {"store_id", "store_name", "city", "state", "region", "store_type", "opened_date"}
        self.assertTrue(expected_fields.issubset(set(reader.fieldnames)))

    def test_products_volume_and_schema(self):
        """Verify products meet volume (>=1000) and schema."""
        path = os.path.join(RAW_DATA_DIR, "products.csv")
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        self.assertGreaterEqual(len(rows), 1000)
        expected_fields = {"product_id", "product_name", "category", "subcategory", "brand", "unit_cost", "recommended_price"}
        self.assertTrue(expected_fields.issubset(set(reader.fieldnames)))

    def test_customers_volume_and_schema(self):
        """Verify customers meet volume (>=10000) and schema."""
        path = os.path.join(RAW_DATA_DIR, "customers.csv")
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        self.assertGreaterEqual(len(rows), 10000)
        expected_fields = {"customer_id", "customer_name", "email", "city", "state", "signup_date", "customer_segment"}
        self.assertTrue(expected_fields.issubset(set(reader.fieldnames)))

    def test_transactions_volume_and_controlled_defects(self):
        """Verify transactions exceed 100,000 rows and contain injected defects."""
        path = os.path.join(RAW_DATA_DIR, "transactions.csv")
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        self.assertGreaterEqual(len(rows), 100000)

        # Audit defect counts
        missing_customers = [r for r in rows if not r["customer_id"].strip()]
        invalid_prices = [r for r in rows if float(r["unit_price"]) <= 0]
        invalid_quantities = [r for r in rows if int(r["quantity"]) <= 0]
        
        # Verify defects were successfully seeded
        self.assertGreater(len(missing_customers), 0, "Missing customer defect not found")
        self.assertGreater(len(invalid_prices), 0, "Invalid price defect not found")
        self.assertGreater(len(invalid_quantities), 0, "Invalid quantity defect not found")

        # Verify duplicate transaction IDs exist
        seen_ids = set()
        dupes = []
        for r in rows:
            tid = r["transaction_id"]
            if tid in seen_ids:
                dupes.append(tid)
            seen_ids.add(tid)
        self.assertGreater(len(dupes), 0, "Duplicate transactions not found")


if __name__ == "__main__":
    unittest.main()
