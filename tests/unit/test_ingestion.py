"""
Unit tests validating Phase 3 Python Ingestion Engine.
"""

import os
import csv
import unittest
from src.ingestion.csv_ingestor import CSVIngestor
from src.ingestion.api_ingestor import APIIngestor
from src.ingestion.postgres_ingestor import PostgresIngestor

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
BRONZE_DIR = os.path.join(PROJECT_ROOT, "data", "bronze")


class TestIngestionEngine(unittest.TestCase):
    """Test suite validating CSV, REST API, and DB ingestors."""

    def test_csv_ingestor_success(self):
        """Verify successful CSV extraction and audit column enrichment."""
        ingestor = CSVIngestor(
            source_name="test_stores",
            required_columns=["store_id", "store_name"]
        )
        records = ingestor.extract(os.path.join(RAW_DIR, "stores.csv"))
        self.assertGreater(len(records), 0)

        enriched = ingestor.add_audit_metadata(records[:5], "stores.csv", batch_id="TEST-BATCH-001")
        self.assertEqual(len(enriched), 5)
        self.assertIn("_ingested_at", enriched[0])
        self.assertIn("_source_file", enriched[0])
        self.assertIn("_batch_id", enriched[0])
        self.assertEqual(enriched[0]["_batch_id"], "TEST-BATCH-001")

    def test_csv_ingestor_missing_file(self):
        """Verify proper FileNotFoundError on missing file."""
        ingestor = CSVIngestor(source_name="non_existent", required_columns=["id"])
        with self.assertRaises(FileNotFoundError):
            ingestor.extract("non_existent_file_path.csv")

    def test_csv_ingestor_schema_mismatch(self):
        """Verify ValueError raised when required column is missing."""
        ingestor = CSVIngestor(
            source_name="test_stores",
            required_columns=["store_id", "non_existent_column_xyz"]
        )
        with self.assertRaises(ValueError):
            ingestor.extract(os.path.join(RAW_DIR, "stores.csv"))

    def test_api_ingestor_payload_transform(self):
        """Verify API response transforms into flat tabular rows."""
        api_ingestor = APIIngestor(api_url="http://mock-api.internal")
        sample_payload = {
            "base": "USD",
            "date": "2026-09-26",
            "rates": {"EUR": 0.92, "GBP": 0.79, "INR": 83.45}
        }
        rows = api_ingestor.transform_payload_to_rows(sample_payload)
        self.assertEqual(len(rows), 4)  # USD base + 3 targets
        currencies = [r["target_currency"] for r in rows]
        self.assertIn("USD", currencies)
        self.assertIn("INR", currencies)

    def test_api_ingestor_offline_fallback(self):
        """Verify API ingestor falls back gracefully to default rates when offline."""
        api_ingestor = APIIngestor(api_url="http://invalid-offline-host:9999/api", max_retries=1, timeout_seconds=1)
        payload = api_ingestor.extract()
        self.assertIn("rates", payload)
        self.assertEqual(payload["base"], "USD")

    def test_postgres_ingestor_fallback(self):
        """Verify PostgresIngestor falls back to local CSV when database is offline."""
        pg_ingestor = PostgresIngestor(
            db_config={"host": "invalid-pg-host", "port": 5432},
            fallback_csv_path=os.path.join(RAW_DIR, "customers.csv")
        )
        records = pg_ingestor.extract()
        self.assertGreater(len(records), 0)
        self.assertEqual(records[0]["customer_id"], "CUST-00001")

    def test_bronze_datasets_exist_with_audit_metadata(self):
        """Verify all 5 Bronze layer tables exist and contain required audit fields."""
        bronze_targets = [
            os.path.join(BRONZE_DIR, "transactions", "transactions_bronze.csv"),
            os.path.join(BRONZE_DIR, "products", "products_bronze.csv"),
            os.path.join(BRONZE_DIR, "stores", "stores_bronze.csv"),
            os.path.join(BRONZE_DIR, "customers", "customers_bronze.csv"),
            os.path.join(BRONZE_DIR, "exchange_rates", "exchange_rates_bronze.csv"),
        ]
        for path in bronze_targets:
            self.assertTrue(os.path.isfile(path), f"Bronze target missing: {path}")
            with open(path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                first_row = next(reader)
                self.assertIn("_ingested_at", first_row)
                self.assertIn("_source_file", first_row)
                self.assertIn("_batch_id", first_row)


if __name__ == "__main__":
    unittest.main()
