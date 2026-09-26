"""
Unit Tests for Master Pipeline Orchestrator (Phase 11).

Validates:
- MasterPipelineOrchestrator stage execution, error propagation, timing,
  and markdown summary report generation.

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
import sys
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from scripts.run_pipeline import MasterPipelineOrchestrator


class TestPipelineOrchestrator(unittest.TestCase):
    """Test suite for Master Pipeline Orchestrator."""

    def setUp(self):
        self.orchestrator = MasterPipelineOrchestrator(run_id="TEST-RUN-001")

    def test_stage_execution_success(self):
        def sample_success_stage():
            return {"count": 100}

        result = self.orchestrator.execute_stage(1, "Mock Stage", sample_success_stage)
        self.assertEqual(result["count"], 100)
        self.assertIn("Mock Stage", self.orchestrator.stage_results)
        self.assertEqual(self.orchestrator.stage_results["Mock Stage"]["status"], "SUCCESS")
        self.assertGreaterEqual(self.orchestrator.stage_results["Mock Stage"]["duration_seconds"], 0.0)

    def test_stage_execution_failure_handling(self):
        def sample_failing_stage():
            raise ValueError("Intentional simulated error")

        with self.assertRaises(ValueError):
            self.orchestrator.execute_stage(2, "Failing Stage", sample_failing_stage)

        self.assertIn("Failing Stage", self.orchestrator.stage_results)
        self.assertEqual(self.orchestrator.stage_results["Failing Stage"]["status"], "FAILED")

    def test_summary_report_generation(self):
        self.orchestrator.stage_results = {
            "Stage A": {"status": "SUCCESS", "duration_seconds": 1.25, "details": {}},
            "Stage B": {"status": "SUCCESS", "duration_seconds": 2.50, "details": {}},
        }
        summary = self.orchestrator.generate_summary_report("SUCCESS", 3.75)
        self.assertEqual(summary["overall_status"], "SUCCESS")
        self.assertEqual(summary["total_duration_seconds"], 3.75)

        summary_doc = os.path.join(PROJECT_ROOT, "docs", "pipeline_execution_summary.md")
        self.assertTrue(os.path.exists(summary_doc))
        with open(summary_doc, "r", encoding="utf-8") as f:
            content = f.read()
            self.assertIn("TEST-RUN-001", content)
            self.assertIn("Stage A", content)
            self.assertIn("SUCCESS", content)


if __name__ == "__main__":
    unittest.main()
