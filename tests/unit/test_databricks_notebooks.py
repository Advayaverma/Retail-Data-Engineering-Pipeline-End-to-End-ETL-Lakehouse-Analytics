"""
Unit Tests for Databricks Compatibility Layer & Notebook Pipeline.

Validates that notebooks execute cleanly in local developer mode,
exercising MockDBUtils, path resolution, and Medallion notebook functions.

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
import sys
import unittest
import importlib.util

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.databricks_utils import is_running_on_databricks, get_lakehouse_path, MockDBUtils, get_dbutils


def load_notebook_module(notebook_name: str):
    file_path = os.path.join(PROJECT_ROOT, "notebooks", f"{notebook_name}.py")
    spec = importlib.util.spec_from_file_location(notebook_name, file_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestDatabricksCompatibility(unittest.TestCase):
    """Test runtime abstraction layer for Databricks."""

    def test_runtime_detection(self):
        # By default in local test env, should detect as local (False)
        self.assertFalse(is_running_on_databricks())

    def test_lakehouse_path_resolution(self):
        bronze_path = get_lakehouse_path("bronze", "transactions.csv")
        self.assertTrue(bronze_path.endswith(os.path.join("data", "bronze", "transactions.csv")))

    def test_mock_dbutils_fs_operations(self):
        dbutils = get_dbutils()
        self.assertIsInstance(dbutils, MockDBUtils)
        
        # Test widget operations
        dbutils.widgets.text("env", "development", "Environment")
        self.assertEqual(dbutils.widgets.get("env"), "development")

    def test_notebook_01_bronze_execution(self):
        nb1 = load_notebook_module("01_bronze_ingestion")
        res = nb1.run_bronze_ingestion()
        self.assertEqual(res["status"], "SUCCESS")
        self.assertIn("transactions.csv", res["counts"])

    def test_notebook_02_silver_execution(self):
        nb2 = load_notebook_module("02_silver_transformation")
        res = nb2.run_silver_pipeline()
        self.assertEqual(res["status"], "SUCCESS")
        self.assertGreater(res["clean_records"], 0)

    def test_notebook_03_gold_execution(self):
        nb3 = load_notebook_module("03_gold_star_schema")
        res = nb3.run_gold_star_schema()
        self.assertEqual(res["status"], "SUCCESS")
        self.assertIn("gold_metrics", res)

    def test_notebook_04_quality_audit_execution(self):
        nb4 = load_notebook_module("04_data_quality_audit")
        res = nb4.audit_data_quality()
        self.assertIn("pass_rate_pct", res)
        self.assertGreater(res["clean_records_passed"], 1000)

    def test_notebook_05_incremental_pipeline_execution(self):
        nb5 = load_notebook_module("05_incremental_pipeline")
        res = nb5.run_incremental_pipeline()
        self.assertEqual(res["status"], "SUCCESS")
        self.assertIn("incremental_summary", res)


if __name__ == "__main__":
    unittest.main()
