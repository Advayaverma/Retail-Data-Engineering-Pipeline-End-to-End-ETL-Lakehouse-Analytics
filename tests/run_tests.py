"""Master test runner using Python standard library unittest.
Executes all unit tests across completed phases with zero external dependencies.
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

from tests.unit.test_foundation import TestProjectFoundation
from tests.unit.test_data_generation import TestDataGeneration
from tests.unit.test_ingestion import TestIngestionEngine
from tests.unit.test_postgres_sql import TestPostgresAndSQL
from tests.unit.test_data_quality import TestDataQualityFramework
from tests.unit.test_transformations import TestTransformations
from tests.unit.test_delta_lake import TestDeltaLakeEngine
from tests.unit.test_star_schema_scd2 import TestStarSchemaAndSCD2
from tests.unit.test_analytics_queries import TestAnalyticsQueries
from tests.unit.test_databricks_notebooks import TestDatabricksCompatibility
from tests.unit.test_orchestrator import TestPipelineOrchestrator
from tests.unit.test_docker import TestDockerConfiguration
from tests.integration.test_end_to_end_pipeline import TestEndToEndLakehousePipeline


def main():
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    suite.addTests(loader.loadTestsFromTestCase(TestProjectFoundation))
    suite.addTests(loader.loadTestsFromTestCase(TestDataGeneration))
    suite.addTests(loader.loadTestsFromTestCase(TestIngestionEngine))
    suite.addTests(loader.loadTestsFromTestCase(TestPostgresAndSQL))
    suite.addTests(loader.loadTestsFromTestCase(TestDataQualityFramework))
    suite.addTests(loader.loadTestsFromTestCase(TestTransformations))
    suite.addTests(loader.loadTestsFromTestCase(TestDeltaLakeEngine))
    suite.addTests(loader.loadTestsFromTestCase(TestStarSchemaAndSCD2))
    suite.addTests(loader.loadTestsFromTestCase(TestAnalyticsQueries))
    suite.addTests(loader.loadTestsFromTestCase(TestDatabricksCompatibility))
    suite.addTests(loader.loadTestsFromTestCase(TestPipelineOrchestrator))
    suite.addTests(loader.loadTestsFromTestCase(TestDockerConfiguration))
    suite.addTests(loader.loadTestsFromTestCase(TestEndToEndLakehousePipeline))

    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)


if __name__ == "__main__":
    main()
