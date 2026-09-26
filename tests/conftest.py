"""Pytest configuration and shared test fixtures."""
import os
import sys
import pytest

# Ensure src directory is in Python path for test discovery
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
SRC_DIR = os.path.join(PROJECT_ROOT, "src")

if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture(scope="session")
def project_root():
    """Return the absolute path to the project root directory."""
    return PROJECT_ROOT


@pytest.fixture(scope="session")
def sample_config():
    """Return a minimal configuration dictionary for testing."""
    return {
        "pipeline": {
            "name": "retail-data-engineering-test",
            "environment": "test",
        },
        "storage": {
            "raw_dir": os.path.join(PROJECT_ROOT, "data", "raw"),
            "bronze_dir": os.path.join(PROJECT_ROOT, "data", "bronze"),
            "silver_dir": os.path.join(PROJECT_ROOT, "data", "silver"),
            "gold_dir": os.path.join(PROJECT_ROOT, "data", "gold"),
            "quarantine_dir": os.path.join(PROJECT_ROOT, "data", "quarantine"),
        },
    }
