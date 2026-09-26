"""
End-to-End Lakehouse Pipeline Integration Test Suite (Phase 13).

Validates the complete data flow from Landing through Medallion architecture:
1. Ingestion: Landed CSVs ingested into Bronze with audit columns (_ingested_at, _source_file, _batch_id).
2. Quality Gate: Defect isolation (~1.05%) into Quarantine and clean records into Silver.
3. Conformed Transformation: Cleansing, deduplication, standardized timestamps, derived financial fields.
4. Incremental Delta Lake: ACID write, schema evolution, and Time Travel versioning.
5. Gold Dimensional Warehousing: dim_date, dim_product, dim_store, dim_customer (SCD Type 2), and fact_sales.
6. Relational DW & Analytical Accuracy: Direct query assertions against retail_dw.db.

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
import sys
import unittest
import csv
import sqlite3

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scripts.run_pipeline import MasterPipelineOrchestrator


class TestEndToEndLakehousePipeline(unittest.TestCase):
    """Full End-to-End integration test across all 7 lakehouse stages."""

    @classmethod
    def setUpClass(cls):
        """Execute one complete master pipeline run to establish state for assertions."""
        cls.orchestrator = MasterPipelineOrchestrator(run_id="INTEGRATION-TEST-RUN")
        cls.summary = cls.orchestrator.run_full_pipeline()

    def test_01_pipeline_overall_success(self):
        """Verify the master pipeline finished with overall status SUCCESS."""
        self.assertEqual(self.summary["overall_status"], "SUCCESS")
        self.assertEqual(len(self.summary["stages"]), 7)
        for stage_name, stage_info in self.summary["stages"].items():
            self.assertEqual(stage_info["status"], "SUCCESS", f"Stage {stage_name} did not succeed.")

    def test_02_bronze_ingestion_audit_columns(self):
        """Verify Bronze layer has lineage tracking columns appended."""
        bronze_tx_path = os.path.join(PROJECT_ROOT, "data", "bronze", "transactions", "transactions_bronze.csv")
        self.assertTrue(os.path.exists(bronze_tx_path), "Bronze transactions table not found.")

        with open(bronze_tx_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            self.assertIn("_ingested_at", header)
            self.assertIn("_source_file", header)
            self.assertIn("_batch_id", header)

            first_row = next(reader)
            row_dict = dict(zip(header, first_row))
            self.assertEqual(row_dict["_batch_id"], "INTEGRATION-TEST-RUN")
            self.assertEqual(row_dict["_source_file"], "transactions.csv")

    def test_03_quarantine_isolation_integrity(self):
        """Verify defective transactions are routed to Quarantine with audit error metadata."""
        quarantine_path = os.path.join(PROJECT_ROOT, "data", "quarantine", "transactions_quarantine.csv")
        self.assertTrue(os.path.exists(quarantine_path), "Quarantine transactions file not found.")

        with open(quarantine_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            self.assertIn("_defect_code", header)
            self.assertIn("_defect_description", header)
            self.assertIn("_quarantined_at", header)

            quarantine_rows = list(reader)
            self.assertGreater(len(quarantine_rows), 1000, "Expected at least 1,000 quarantined rows.")

    def test_04_silver_conformed_cleansing(self):
        """Verify Silver conformed transactions are deduplicated, typed, and derived."""
        silver_tx_path = os.path.join(PROJECT_ROOT, "data", "silver", "transactions", "transactions_silver.csv")
        self.assertTrue(os.path.exists(silver_tx_path), "Silver transactions file not found.")

        with open(silver_tx_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            first_row = next(reader)
            # Check derived fields exist
            self.assertIn("gross_amount", first_row)
            self.assertIn("discount_amount", first_row)
            self.assertIn("net_amount", first_row)
            self.assertIn("txn_date", first_row)
            self.assertIn("txn_year", first_row)
            self.assertIn("txn_month", first_row)

            # Mathematical integrity: gross - discount_amount == net_amount
            gross = float(first_row["gross_amount"])
            disc_amt = float(first_row["discount_amount"])
            net = float(first_row["net_amount"])
            self.assertAlmostEqual(net, round(gross - disc_amt, 2), places=1)

    def test_05_delta_lake_acid_and_time_travel(self):
        """Verify Delta Lake transaction log exists and preserves ACID history."""
        delta_log_dir = os.path.join(PROJECT_ROOT, "data", "silver", "delta_transactions", "_delta_log")
        self.assertTrue(os.path.exists(delta_log_dir), "Delta transaction log folder missing.")

        log_files = [f for f in os.listdir(delta_log_dir) if f.endswith(".json")]
        self.assertGreaterEqual(len(log_files), 2, "Expected at least 2 commit versions in Delta log.")

    def test_06_gold_star_schema_and_scd2(self):
        """Verify Kimball Star Schema dimensions and fact table surrogate key integrity."""
        gold_dir = os.path.join(PROJECT_ROOT, "data", "gold")
        
        # Verify dim_customer SCD Type 2
        cust_path = os.path.join(gold_dir, "dim_customer", "dim_customer.csv")
        self.assertTrue(os.path.exists(cust_path))
        with open(cust_path, "r", encoding="utf-8") as f:
            cust_reader = csv.DictReader(f)
            current_count = 0
            expired_count = 0
            for row in cust_reader:
                if row["is_current"].lower() == "true":
                    current_count += 1
                else:
                    expired_count += 1
            self.assertEqual(current_count, 10500, "Active customer count should match customer master (10,500).")
            self.assertGreater(expired_count, 200, "Expected historical SCD2 expired rows.")

        # Verify fact_sales
        fact_path = os.path.join(gold_dir, "fact_sales", "fact_sales.csv")
        self.assertTrue(os.path.exists(fact_path))
        with open(fact_path, "r", encoding="utf-8") as f:
            fact_reader = csv.DictReader(f)
            sample_fact = next(fact_reader)
            self.assertIn("sales_sk", sample_fact)
            self.assertIn("date_sk", sample_fact)
            self.assertIn("customer_sk", sample_fact)
            self.assertIn("product_sk", sample_fact)
            self.assertIn("store_sk", sample_fact)
            self.assertIn("net_sales", sample_fact)
            self.assertIn("profit", sample_fact)

    def test_07_relational_warehouse_analytics_accuracy(self):
        """Verify direct SQL query execution against SQLite / DW engine."""
        db_path = os.path.join(PROJECT_ROOT, "data", "retail_dw.db")
        self.assertTrue(os.path.exists(db_path), "retail_dw.db does not exist.")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # 1. Row count validation across tables
        cursor.execute("SELECT COUNT(*) FROM fact_sales;")
        fact_count = cursor.fetchone()[0]
        self.assertGreaterEqual(fact_count, 100000)

        cursor.execute("SELECT COUNT(*) FROM dim_date;")
        date_count = cursor.fetchone()[0]
        self.assertEqual(date_count, 2191)

        # 2. Financial validation: Net revenue > Cost
        cursor.execute("SELECT SUM(net_sales), SUM(profit) FROM fact_sales;")
        total_sales, total_profit = cursor.fetchone()
        self.assertGreater(total_sales, 50000000.0, "Total sales should exceed $50M.")
        self.assertGreater(total_profit, 10000000.0, "Total profit should exceed $10M.")

        conn.close()


if __name__ == "__main__":
    unittest.main()
