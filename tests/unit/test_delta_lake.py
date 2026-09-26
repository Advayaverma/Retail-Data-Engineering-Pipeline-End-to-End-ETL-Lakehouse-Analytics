"""
Unit tests validating Phase 7 Delta Lake Engine, ACID commits, MERGE upserts, and Time Travel.
"""

import os
import shutil
import unittest
from src.transformation.delta_lake_engine import DeltaTable

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEST_DELTA_DIR = os.path.join(PROJECT_ROOT, "data", "test_delta_table")
REAL_DELTA_DIR = os.path.join(PROJECT_ROOT, "data", "silver", "delta_transactions")


class TestDeltaLakeEngine(unittest.TestCase):
    """Test suite validating Delta Lake ACID storage, Schema Enforcement, MERGE, and Time Travel."""

    def setUp(self):
        os.makedirs(TEST_DELTA_DIR, exist_ok=True)
        self.table = DeltaTable(table_path=TEST_DELTA_DIR)

    def tearDown(self):
        if os.path.isdir(TEST_DELTA_DIR):
            shutil.rmtree(TEST_DELTA_DIR, ignore_errors=True)

    def test_acid_commit_log_creation(self):
        """Verify write operation creates sequential commit log in _delta_log/."""
        records = [
            {"id": "1", "name": "Item A", "amount": "100.0"},
            {"id": "2", "name": "Item B", "amount": "200.0"},
        ]
        version = self.table.write(records, mode="overwrite")
        self.assertEqual(version, 0)

        log_path = os.path.join(TEST_DELTA_DIR, "_delta_log", "00000000000000000000.json")
        self.assertTrue(os.path.isfile(log_path))

        history = self.table.get_history()
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["version"], 0)
        self.assertEqual(history[0]["operation"], "WRITE")

    def test_schema_enforcement_rejection(self):
        """Verify schema enforcement blocks mismatched column writes."""
        v0_records = [{"id": "1", "name": "Item A"}]
        self.table.write(v0_records, mode="overwrite")

        # Incompatible record with new column when merge_schema=False
        v1_incompatible = [{"id": "2", "name": "Item B", "unexpected_column": "value"}]
        with self.assertRaises(ValueError) as ctx:
            self.table.write(v1_incompatible, mode="append", merge_schema=False)
        self.assertIn("schema mismatch", str(ctx.exception).lower())

    def test_schema_evolution_success(self):
        """Verify schema evolution integrates new columns when merge_schema=True."""
        v0_records = [{"id": "1", "name": "Item A"}]
        self.table.write(v0_records, mode="overwrite")

        v1_records = [{"id": "2", "name": "Item B", "new_feature": "points_100"}]
        v1 = self.table.write(v1_records, mode="append", merge_schema=True)
        self.assertEqual(v1, 1)

        read_v1 = self.table.read_version(1)
        self.assertEqual(len(read_v1), 1)
        self.assertIn("new_feature", read_v1[0])

    def test_delta_merge_upsert(self):
        """Verify Delta MERGE updates matched records and inserts unmatched records."""
        initial_data = [
            {"id": "1", "qty": "2", "status": "PENDING"},
            {"id": "2", "qty": "5", "status": "PENDING"},
        ]
        self.table.write(initial_data, mode="overwrite")

        # Day 2 delta: update id 1 to COMPLETED, insert new id 3
        delta_batch = [
            {"id": "1", "qty": "2", "status": "COMPLETED"},  # Matched -> update
            {"id": "3", "qty": "1", "status": "NEW"},        # Unmatched -> insert
        ]
        updated, inserted, v1 = self.table.merge(delta_batch, key_field="id", merge_schema=True)
        self.assertEqual(updated, 1)
        self.assertEqual(inserted, 1)
        self.assertEqual(v1, 1)

        merged_data = self.table.read_version(1)
        self.assertEqual(len(merged_data), 3)

        id1_row = next(r for r in merged_data if r["id"] == "1")
        self.assertEqual(id1_row["status"], "COMPLETED")

    def test_time_travel_historical_snapshot(self):
        """Verify Time Travel restores earlier table versions."""
        v0_data = [{"id": "1", "price": "10.00"}]
        self.table.write(v0_data, mode="overwrite")

        v1_data = [{"id": "1", "price": "99.99"}]
        self.table.write(v1_data, mode="overwrite")

        # Query version 0
        v0_snapshot = self.table.read_version(0)
        self.assertEqual(v0_snapshot[0]["price"], "10.00")

        # Query version 1
        v1_snapshot = self.table.read_version(1)
        self.assertEqual(v1_snapshot[0]["price"], "99.99")

    def test_persisted_delta_transactions_table(self):
        """Verify production delta_transactions table exists with commit log version 0 and 1."""
        self.assertTrue(os.path.isdir(REAL_DELTA_DIR))
        log_v0 = os.path.join(REAL_DELTA_DIR, "_delta_log", "00000000000000000000.json")
        log_v1 = os.path.join(REAL_DELTA_DIR, "_delta_log", "00000000000000000001.json")
        self.assertTrue(os.path.isfile(log_v0), "Version 0 Delta commit log missing")
        self.assertTrue(os.path.isfile(log_v1), "Version 1 Delta commit log missing")


if __name__ == "__main__":
    unittest.main()
