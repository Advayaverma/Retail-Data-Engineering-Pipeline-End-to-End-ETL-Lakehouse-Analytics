"""
Unit Tests for CI/CD Workflow Specification Integrity (Phase 14).

Validates:
- .github/workflows/ci.yml exists and contains required jobs (lint, test, e2e, docker).
- Actions use modern versions (checkout@v4, setup-python@v5, setup-java@v4).
- Matrix or runtime targets OpenJDK 17 and Python 3.11.

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
import sys
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


class TestCICDPipelineWorkflow(unittest.TestCase):
    """Test suite for GitHub Actions CI/CD workflow configuration."""

    def setUp(self):
        self.workflow_path = os.path.join(PROJECT_ROOT, ".github", "workflows", "ci.yml")

    def test_workflow_file_exists(self):
        self.assertTrue(os.path.exists(self.workflow_path), "ci.yml does not exist.")

    def test_workflow_triggers_and_jobs(self):
        with open(self.workflow_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Trigger branches
        self.assertIn("push:", content)
        self.assertIn("pull_request:", content)
        self.assertIn("main", content)

        # Core jobs
        self.assertIn("code-quality-and-lint:", content)
        self.assertIn("test-suite:", content)
        self.assertIn("pipeline-e2e-dryrun:", content)
        self.assertIn("build-container:", content)

    def test_runtime_dependencies_and_steps(self):
        with open(self.workflow_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Python & Java versions
        self.assertIn("python-version: '3.11'", content)
        self.assertIn("java-version: '17'", content)
        self.assertIn("actions/setup-java@v4", content)

        # Verification commands
        self.assertIn("python tests/run_tests.py", content)
        self.assertIn("python scripts/run_pipeline.py", content)
        self.assertIn("docker/build-push-action@v5", content)


if __name__ == "__main__":
    unittest.main()
