"""
Unit Tests for Azure Cloud Extension & Architecture Specifications (Phase 15).

Validates:
- docs/azure_cloud_architecture.md exists and contains enterprise Azure service mappings
  (ADF, ADLS Gen2, Azure Databricks, Unity Catalog, Azure Synapse, Key Vault).
- Terraform infrastructure-as-code blueprint is defined.
- Cost optimization policies (Spot instances, Serverless auto-suspend, lifecycle policies) are documented.

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
import sys
import unittest

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


class TestAzureCloudArchitecture(unittest.TestCase):
    """Test suite for Azure Cloud mapping and architecture documentation."""

    def setUp(self):
        self.doc_path = os.path.join(PROJECT_ROOT, "docs", "azure_cloud_architecture.md")

    def test_document_exists(self):
        self.assertTrue(os.path.exists(self.doc_path), "azure_cloud_architecture.md does not exist.")

    def test_azure_services_mapping(self):
        with open(self.doc_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check core Azure services
        self.assertIn("Azure Data Factory", content)
        self.assertIn("Azure Data Lake Storage Gen2", content)
        self.assertIn("Azure Databricks", content)
        self.assertIn("Unity Catalog", content)
        self.assertIn("Azure Synapse Analytics", content)
        self.assertIn("Azure Key Vault", content)
        self.assertIn("Microsoft Entra ID", content)

    def test_terraform_iac_blueprint(self):
        with open(self.doc_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("azurerm_storage_account", content)
        self.assertIn("is_hns_enabled", content)
        self.assertIn("azurerm_databricks_workspace", content)
        self.assertIn("azurerm_key_vault", content)

    def test_cost_and_governance(self):
        with open(self.doc_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("Spot Instances", content)
        self.assertIn("Liquid Clustering", content)
        self.assertIn("VACUUM", content)


if __name__ == "__main__":
    unittest.main()
