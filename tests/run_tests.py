"""Test runner using Python standard library unittest.
Allows running foundational and unit tests without external dependencies.
"""
import os
import sys
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(PROJECT_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


class TestProjectFoundation(unittest.TestCase):
    """Test suite validating Phase 1 repository structure and configuration."""

    def test_directory_structure(self):
        required_dirs = [
            os.path.join(PROJECT_ROOT, "data", "raw"),
            os.path.join(PROJECT_ROOT, "data", "bronze"),
            os.path.join(PROJECT_ROOT, "data", "silver"),
            os.path.join(PROJECT_ROOT, "data", "gold"),
            os.path.join(PROJECT_ROOT, "data", "quarantine"),
            os.path.join(PROJECT_ROOT, "src", "ingestion"),
            os.path.join(PROJECT_ROOT, "src", "transformation"),
            os.path.join(PROJECT_ROOT, "src", "validation"),
            os.path.join(PROJECT_ROOT, "src", "modelling"),
            os.path.join(PROJECT_ROOT, "src", "utils"),
            os.path.join(PROJECT_ROOT, "pyspark", "bronze"),
            os.path.join(PROJECT_ROOT, "pyspark", "silver"),
            os.path.join(PROJECT_ROOT, "pyspark", "gold"),
            os.path.join(PROJECT_ROOT, "pyspark", "incremental"),
            os.path.join(PROJECT_ROOT, "sql", "init"),
            os.path.join(PROJECT_ROOT, "sql", "staging"),
            os.path.join(PROJECT_ROOT, "sql", "transformations"),
            os.path.join(PROJECT_ROOT, "sql", "analytics"),
            os.path.join(PROJECT_ROOT, "config"),
            os.path.join(PROJECT_ROOT, "notebooks"),
            os.path.join(PROJECT_ROOT, "scripts"),
            os.path.join(PROJECT_ROOT, "docker", "postgres"),
            os.path.join(PROJECT_ROOT, ".github", "workflows"),
        ]
        for path in required_dirs:
            self.assertTrue(os.path.isdir(path), f"Missing required directory: {path}")

    def test_essential_files_exist(self):
        required_files = [
            os.path.join(PROJECT_ROOT, ".gitignore"),
            os.path.join(PROJECT_ROOT, ".dockerignore"),
            os.path.join(PROJECT_ROOT, ".env.example"),
            os.path.join(PROJECT_ROOT, "requirements.txt"),
            os.path.join(PROJECT_ROOT, "Dockerfile"),
            os.path.join(PROJECT_ROOT, "docker-compose.yml"),
            os.path.join(PROJECT_ROOT, "README.md"),
            os.path.join(PROJECT_ROOT, "config", "pipeline_config.yaml"),
            os.path.join(PROJECT_ROOT, ".github", "workflows", "ci.yml"),
            os.path.join(PROJECT_ROOT, "docker", "postgres", "init.sql"),
        ]
        for file_path in required_files:
            self.assertTrue(os.path.isfile(file_path), f"Missing required file: {file_path}")

    def test_config_readable(self):
        config_path = os.path.join(PROJECT_ROOT, "config", "pipeline_config.yaml")
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("pipeline:", content)
        self.assertIn("retail-data-engineering-pipeline", content)
        self.assertIn("storage:", content)
        self.assertIn("sources:", content)


if __name__ == "__main__":
    runner = unittest.TextTestRunner(verbosity=2)
    suite = unittest.TestLoader().loadTestsFromTestCase(TestProjectFoundation)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
