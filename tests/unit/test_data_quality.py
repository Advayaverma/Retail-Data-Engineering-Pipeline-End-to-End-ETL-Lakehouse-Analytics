"""
Unit tests validating Phase 5 Data Quality Rules Engine, Quarantine Isolation, and Audit Reporting.
"""

import os
import unittest
from src.validation.rules import (
    NotNullOrEmptyRule,
    NumericRangeRule,
    TimestampFormatRule,
    ReferentialIntegrityRule,
    DuplicateTracker,
)
from src.validation.quality_checker import DataQualityChecker

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
QUARANTINE_DIR = os.path.join(PROJECT_ROOT, "data", "quarantine")


class TestDataQualityFramework(unittest.TestCase):
    """Test suite validating quality rules, duplicate tracking, and quarantine routing."""

    def test_not_null_rule(self):
        """Verify NotNullOrEmptyRule catches null and empty strings."""
        rule = NotNullOrEmptyRule("cust_id", "ERR_NULL")
        self.assertTrue(rule.evaluate({"cust_id": "CUST-001"}).is_valid)
        self.assertFalse(rule.evaluate({"cust_id": ""}).is_valid)
        self.assertFalse(rule.evaluate({"cust_id": "   "}).is_valid)
        self.assertFalse(rule.evaluate({"other_key": "val"}).is_valid)

    def test_numeric_range_rule(self):
        """Verify NumericRangeRule validates bounds and catches non-numeric types."""
        rule = NumericRangeRule("price", min_val=0.01, max_val=1000.0, error_code="ERR_PRICE")
        self.assertTrue(rule.evaluate({"price": "19.99"}).is_valid)
        self.assertTrue(rule.evaluate({"price": 500}).is_valid)
        # Out of bounds
        self.assertFalse(rule.evaluate({"price": "-5.00"}).is_valid)
        self.assertFalse(rule.evaluate({"price": "0.00"}).is_valid)
        self.assertFalse(rule.evaluate({"price": "1500.00"}).is_valid)
        # Corrupt type
        self.assertFalse(rule.evaluate({"price": "not_a_number"}).is_valid)

    def test_timestamp_rule(self):
        """Verify TimestampFormatRule validates format and detects invalid calendar dates."""
        rule = TimestampFormatRule("txn_time", "%Y-%m-%d %H:%M:%S", "ERR_TS")
        self.assertTrue(rule.evaluate({"txn_time": "2025-06-15 14:30:00"}).is_valid)
        self.assertFalse(rule.evaluate({"txn_time": "2024/02/31 25:99:99"}).is_valid)
        self.assertFalse(rule.evaluate({"txn_time": "INVALID_DATE"}).is_valid)
        self.assertFalse(rule.evaluate({"txn_time": ""}).is_valid)

    def test_referential_integrity_rule(self):
        """Verify ReferentialIntegrityRule validates foreign keys against reference sets."""
        valid_customers = {"CUST-001", "CUST-002", "CUST-003"}
        rule = ReferentialIntegrityRule("customer_id", valid_customers, "customers", "ERR_REF")
        self.assertTrue(rule.evaluate({"customer_id": "CUST-001"}).is_valid)
        self.assertFalse(rule.evaluate({"customer_id": "CUST-999"}).is_valid)
        self.assertFalse(rule.evaluate({"customer_id": ""}).is_valid)

    def test_duplicate_tracker(self):
        """Verify DuplicateTracker flags repeated primary keys."""
        tracker = DuplicateTracker("transaction_id")
        self.assertTrue(tracker.check_duplicate({"transaction_id": "TXN-001"}).is_valid)
        self.assertTrue(tracker.check_duplicate({"transaction_id": "TXN-002"}).is_valid)
        # Second instance must fail
        dupe_result = tracker.check_duplicate({"transaction_id": "TXN-001"})
        self.assertFalse(dupe_result.is_valid)
        self.assertEqual(dupe_result.error_code, "ERR_DUPLICATE_RECORD")

    def test_quality_checker_reconciliation(self):
        """Verify DataQualityChecker partitions data cleanly without dropping records."""
        sample_data = [
            {"id": "1", "price": "10.00", "ts": "2025-01-01 10:00:00"},
            {"id": "2", "price": "-5.00", "ts": "2025-01-01 10:00:00"},   # Bad price
            {"id": "3", "price": "15.00", "ts": "INVALID_TIMESTAMP"},      # Bad ts
            {"id": "1", "price": "10.00", "ts": "2025-01-01 10:00:00"},   # Dupe of 1
        ]
        checker = DataQualityChecker("test_set")
        checker.set_duplicate_tracker("id")
        checker.add_rule(NumericRangeRule("price", min_val=0.01, error_code="ERR_PRICE"))
        checker.add_rule(TimestampFormatRule("ts", "%Y-%m-%d %H:%M:%S", error_code="ERR_TS"))

        clean, quarantine, report = checker.validate_dataset(sample_data)
        self.assertEqual(len(clean), 1)
        self.assertEqual(len(quarantine), 3)
        self.assertEqual(report["rows_processed"], 4)
        self.assertEqual(report["valid_rows"], 1)
        self.assertEqual(report["quarantine_rows"], 3)
        # Check audit fields exist on quarantined rows
        self.assertIn("_defect_code", quarantine[0])
        self.assertIn("_defect_description", quarantine[0])
        self.assertIn("_quarantined_at", quarantine[0])

    def test_quarantine_file_generated_and_populated(self):
        """Verify the physical transactions_quarantine.csv exists and has rows."""
        q_path = os.path.join(QUARANTINE_DIR, "transactions_quarantine.csv")
        self.assertTrue(os.path.isfile(q_path), f"Quarantine file missing: {q_path}")
        with open(q_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        self.assertGreater(len(lines), 100, "Quarantine file should contain all flagged defects")


if __name__ == "__main__":
    unittest.main()
