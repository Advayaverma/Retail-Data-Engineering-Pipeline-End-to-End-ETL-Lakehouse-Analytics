"""
Unit Tests for Docker Configuration and Deployment Integrity (Phase 12).

Validates:
- Dockerfile exists and contains required instructions (base image, OpenJDK, requirements, COPY, CMD).
- docker-compose.yml exists and contains required service definitions, healthchecks, and volume mappings.
- docker/postgres/init.sql exists and creates required schemas and source tables.

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
import sys
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


class TestDockerConfiguration(unittest.TestCase):
    """Test suite for Docker & Docker Compose setup."""

    def test_dockerfile_contents(self):
        dockerfile_path = os.path.join(PROJECT_ROOT, "Dockerfile")
        self.assertTrue(os.path.exists(dockerfile_path), "Dockerfile does not exist.")

        with open(dockerfile_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("FROM python:3.11-slim", content)
        self.assertIn("openjdk-17-jre-headless", content)
        self.assertIn("ENV JAVA_HOME=", content)
        self.assertIn("COPY requirements.txt", content)
        self.assertIn("pip install", content)
        self.assertIn("CMD [\"python\", \"scripts/run_pipeline.py\"]", content)

    def test_docker_compose_structure(self):
        compose_path = os.path.join(PROJECT_ROOT, "docker-compose.yml")
        self.assertTrue(os.path.exists(compose_path), "docker-compose.yml does not exist.")

        with open(compose_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Validate postgres service
        self.assertIn("services:", content)
        self.assertIn("postgres:", content)
        self.assertIn("image: postgres:16-alpine", content)
        self.assertIn("healthcheck:", content)
        self.assertIn("pg_isready", content)

        # Validate etl_app service
        self.assertIn("etl_app:", content)
        self.assertIn("depends_on:", content)
        self.assertIn("condition: service_healthy", content)
        self.assertIn("./data:/app/data", content)

    def test_postgres_init_sql(self):
        init_sql_path = os.path.join(PROJECT_ROOT, "docker", "postgres", "init.sql")
        self.assertTrue(os.path.exists(init_sql_path), "docker/postgres/init.sql does not exist.")

        with open(init_sql_path, "r", encoding="utf-8") as f:
            sql_content = f.read()

        self.assertIn("retail_source", sql_content)
        self.assertIn("retail_dw", sql_content)
        self.assertIn("retail_source.stores", sql_content)
        self.assertIn("retail_source.products", sql_content)
        self.assertIn("retail_source.customers", sql_content)
        self.assertIn("retail_source.transactions", sql_content)


if __name__ == "__main__":
    unittest.main()
