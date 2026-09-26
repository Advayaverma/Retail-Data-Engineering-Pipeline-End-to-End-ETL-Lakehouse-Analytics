"""Foundational tests verifying project structure and configuration loading."""
import os
import yaml


def test_directory_structure_exists(project_root):
    """Verify that all core Medallion Lakehouse directories exist."""
    required_dirs = [
        os.path.join(project_root, "data", "raw"),
        os.path.join(project_root, "data", "bronze"),
        os.path.join(project_root, "data", "silver"),
        os.path.join(project_root, "data", "gold"),
        os.path.join(project_root, "data", "quarantine"),
        os.path.join(project_root, "src", "ingestion"),
        os.path.join(project_root, "src", "transformation"),
        os.path.join(project_root, "src", "validation"),
        os.path.join(project_root, "src", "modelling"),
        os.path.join(project_root, "src", "utils"),
        os.path.join(project_root, "pyspark"),
        os.path.join(project_root, "sql"),
        os.path.join(project_root, "config"),
    ]
    for d in required_dirs:
        assert os.path.exists(d), f"Required directory does not exist: {d}"


def test_pipeline_config_yaml(project_root):
    """Verify pipeline configuration file can be parsed and contains required keys."""
    config_path = os.path.join(project_root, "config", "pipeline_config.yaml")
    assert os.path.isfile(config_path), f"Configuration file missing: {config_path}"
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    assert "pipeline" in config
    assert "storage" in config
    assert "sources" in config
    assert "spark" in config
    assert config["pipeline"]["name"] == "retail-data-engineering-pipeline"
