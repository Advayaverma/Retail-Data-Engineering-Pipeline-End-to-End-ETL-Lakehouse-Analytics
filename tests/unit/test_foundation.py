"""Foundational tests verifying project structure and configuration loading."""
import os
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestProjectFoundation(unittest.TestCase):
    """Test suite validating Phase 1 repository structure and configuration."""

    def test_directory_structure(self):
        """Verify that all core Medallion Lakehouse directories exist."""
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
        for d in required_dirs:
            self.assertTrue(os.path.isdir(d), f"Required directory does not exist: {d}")

    def test_essential_files_exist(self):
        """Verify that all essential foundation files are present."""
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

    def test_pipeline_config_content(self):
        """Verify pipeline configuration file can be opened and contains expected key sections."""
        config_path = os.path.join(PROJECT_ROOT, "config", "pipeline_config.yaml")
        self.assertTrue(os.path.isfile(config_path), f"Configuration file missing: {config_path}")
        with open(config_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("pipeline:", content)
        self.assertIn("retail-data-engineering-pipeline", content)
        self.assertIn("storage:", content)
        self.assertIn("sources:", content)
        self.assertIn("spark:", content)


if __name__ == "__main__":
    unittest.main()
