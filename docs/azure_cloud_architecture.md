# Enterprise Azure Lakehouse Cloud Architecture Guide
**Project:** Retail Data Engineering Pipeline — End-to-End Lakehouse Analytics  
**Target Organization:** Celebal Technologies (Premier Microsoft & Databricks Cloud Partner)  
**Cloud Ecosystem:** Microsoft Azure (ADF, ADLS Gen2, Azure Databricks, Unity Catalog, Azure Synapse Analytics, Key Vault, Monitor)

---

## 1. Executive Summary & Cloud Migration Rationale

Celebal Technologies specializes in architecting enterprise-grade, cloud-native data platforms on **Microsoft Azure** and **Databricks**. While this project runs locally with complete fidelity (standard library fallbacks, local Delta Lake ACID simulator, and SQLite warehouse), every single component was deliberately engineered to map directly to an equivalent **Azure Enterprise Service**.

This document outlines the end-to-end cloud reference architecture, mapping local components to enterprise Azure services, defining security, networking, cost optimization, disaster recovery, and infrastructure-as-code (Terraform) specifications.

```
       +-----------------------------------------------------------------------------------------+
       |                          Microsoft Azure Cloud Platform                                 |
       |                                                                                         |
       |   [On-Prem / APIs / DBs]                                                                |
       |             |                                                                           |
       |             v                                                                           |
       |   +--------------------+     +------------------------------------------------------+   |
       |   | Azure Data Factory |---->| Azure Data Lake Storage Gen2 (ADLS)                  |   |
       |   | (ADF Orchestrator) |     | - abfss://landing@retaildls.dfs.core.windows.net/    |   |
       |   +--------------------+     | - abfss://bronze@retaildls.dfs.core.windows.net/     |   |
       |             |                | - abfss://silver@retaildls.dfs.core.windows.net/     |   |
       |             | (Webhooks/     | - abfss://gold@retaildls.dfs.core.windows.net/       |   |
       |             |  Triggers)     +---------------------------+--------------------------+   |
       |             v                                            |                              |
       |   +------------------------------------------------+     |                              |
       |   | Azure Databricks (Unified Analytics Platform)  |<----+ (Delta Lake Tables)          |
       |   | - 01_bronze_ingestion                          |                                    |
       |   | - 02_silver_transformation (Quarantine Router) |                                    |
       |   | - 03_gold_star_schema (Kimball SCD Type 2)     |                                    |
       |   | - 05_incremental_merge (ACID / Time Travel)    |                                    |
       |   | Unity Catalog Governance (Three-level namespace)|                                   |
       |   +------------------------+-----------------------+                                    |
       |                            |                                                            |
       |                            v                                                            |
       |   +------------------------------------------------+                                    |
       |   | Azure Synapse Analytics / Power BI DirectLake  |                                    |
       |   | (Executive Dashboards & BI Semantic Model)     |                                    |
       |   +------------------------------------------------+                                    |
       |                                                                                         |
       |   Cross-Cutting Services:                                                               |
       |   - Azure Key Vault (Secrets & Managed Identities)                                      |
       |   - Azure Monitor & Log Analytics (Observability & Alerts)                              |
       |   - Microsoft Entra ID (Role-Based Access Control)                                      |
       +-----------------------------------------------------------------------------------------+
```

---

## 2. Local Component to Azure Cloud Mapping Matrix

| Local Component | Code / Artifact | Azure Enterprise Cloud Equivalent | Architectural Role & Implementation Details |
|---|---|---|---|
| **Raw Feeds & Mock API** | `data/raw/`, `scripts/mock_api_server.py` | **Azure Data Factory (ADF) & Event Hubs** | Copy Activities pull transactional dumps via SFTP/REST; Event Hubs captures real-time POS streams. |
| **Bronze Layer** | `data/bronze/`, `01_bronze_ingestion.py` | **ADLS Gen2 (`bronze` container) + Delta** | Raw, append-only landing zone. Adds system lineage columns (`_ingested_at`, `_source_file`, `_batch_id`). |
| **Data Quality Gate** | `src/validation/quality_checker.py`, `rules.py` | **Databricks Delta Live Tables (DLT) / Great Expectations** | Strict expectations (`@dlt.expect_or_drop`). Non-compliant records quarantined to `quarantine` container. |
| **Silver Layer** | `data/silver/`, `02_silver_transformation.py` | **ADLS Gen2 (`silver` container) + Delta** | Cleaned, deduplicated, standardized schemas with conformed business rules and parsed datetimes. |
| **Incremental Engine** | `src/transformation/delta_lake_engine.py` | **Delta Lake ACID & Auto Loader (`cloudFiles`)** | High-throughput streaming ingestion with atomic `MERGE INTO`, schema evolution, and Time Travel audit. |
| **Gold Star Schema** | `src/modelling/star_schema_builder.py`, `scd2_handler.py` | **Azure Databricks SQL Serverless / Synapse** | Kimball Star Schema: `dim_date`, `dim_product`, `dim_store`, `dim_customer` (SCD Type 2), `fact_sales`. |
| **Relational DW** | `data/retail_dw.db`, `scripts/load_gold_dw.py` | **Azure Synapse Dedicated SQL Pool / Fabric Warehouse** | High-concurrency MPP relational warehouse indexed by distribution and clustered columns. |
| **Orchestrator** | `scripts/run_pipeline.py` | **Azure Data Factory Pipeline / Databricks Workflows** | Directed Acyclic Graph (DAG) task chaining, retry policies, dependency management, and notifications. |
| **Secrets & Config** | `.env`, `config/pipeline_config.yaml` | **Azure Key Vault & App Configuration** | Zero plain-text credentials; Azure RBAC with Managed Identities (`DefaultAzureCredential`). |
| **Observability** | `src/utils/logger.py` | **Azure Monitor & Log Analytics Workspace** | Centralized diagnostic logging, KQL telemetry queries, SLA failure alerts via Action Groups. |

---

## 3. Storage Layer: ADLS Gen2 Hierarchical Namespace & Security

Azure Data Lake Storage Gen2 combines the low cost of Azure Blob storage with high-performance POSIX file permissions:

```
abfss://retail-lakehouse@retailadlsgen2.dfs.core.windows.net/
├── landing/           # Raw, untransformed source drops from POS and APIs
├── bronze/            # Append-only raw delta tables with lineage metadata
│   ├── transactions/
│   ├── customers/
│   ├── products/
│   └── stores/
├── quarantine/        # Quarantined rows failing atomic data quality rules
│   └── transactions_quarantine/
├── silver/            # Conformed, typed, and deduplicated delta tables
│   ├── transactions/
│   └── delta_transactions/
└── gold/              # Kimball star schema dimensions and fact tables
    ├── dim_date/
    ├── dim_product/
    ├── dim_store/
    ├── dim_customer/   # SCD Type 2 active + historical snapshots
    └── fact_sales/
```

### Security & Access Control
- **Azure Entra ID (Azure AD)** integration with Azure RBAC (`Storage Blob Data Contributor`).
- **Private Endpoints (Private Link)**: Eliminates public internet exposure by routing traffic across Azure virtual network (VNet) backbone.
- **Customer-Managed Keys (CMK)**: Encrypted at rest via Azure Key Vault with 256-bit AES encryption.

---

## 4. Compute & Governance: Azure Databricks & Unity Catalog

### 4.1 Unity Catalog 3-Level Namespace
Databricks Unity Catalog provides centralized governance across multi-cloud environments:

```
Catalog: retail_catalog
├── Schema: bronze
│   └── Table: transactions_raw (Delta)
├── Schema: silver
│   └── Table: transactions_silver (Delta, Liquid Clustered)
└── Schema: gold
    ├── Table: dim_date (Delta)
    ├── Table: dim_customer (Delta, SCD Type 2)
    ├── Table: dim_product (Delta)
    ├── Table: dim_store (Delta)
    └── Table: fact_sales (Delta, Partitioned/Clustered)
```

### 4.2 Liquid Clustering vs. Legacy Partitioning
In production on Azure Databricks (Runtime 13.3+):
```sql
-- Replaces traditional PARTITIONED BY (store_id) to eliminate small-file skew
CREATE OR REPLACE TABLE retail_catalog.silver.transactions_silver
CLUSTER BY (store_id, transaction_timestamp)
AS SELECT * FROM bronze_cleansed;
```

---

## 5. End-to-End ADF Pipeline Orchestration Specification

In Azure, the master pipeline is orchestrated via **Azure Data Factory** calling **Azure Databricks Linked Services**:

```json
{
  "name": "Pipeline_Retail_Lakehouse_Daily_ETL",
  "properties": {
    "activities": [
      {
        "name": "Copy_POS_Landing_to_Bronze",
        "type": "Copy",
        "inputs": [{ "referenceName": "ADLS_Landing_CSV", "type": "DatasetReference" }],
        "outputs": [{ "referenceName": "ADLS_Bronze_CSV", "type": "DatasetReference" }]
      },
      {
        "name": "Execute_Databricks_Silver_Transformation",
        "type": "DatabricksNotebook",
        "dependsOn": [{ "activity": "Copy_POS_Landing_to_Bronze", "dependencyConditions": ["Succeeded"] }],
        "linkedServiceName": { "referenceName": "AzureDatabricksLinkedService", "type": "LinkedServiceReference" },
        "typeProperties": {
          "notebookPath": "/Repos/Production/retail-pipeline/notebooks/02_silver_transformation"
        }
      },
      {
        "name": "Execute_Databricks_Gold_Star_Schema",
        "type": "DatabricksNotebook",
        "dependsOn": [{ "activity": "Execute_Databricks_Silver_Transformation", "dependencyConditions": ["Succeeded"] }],
        "linkedServiceName": { "referenceName": "AzureDatabricksLinkedService", "type": "LinkedServiceReference" },
        "typeProperties": {
          "notebookPath": "/Repos/Production/retail-pipeline/notebooks/03_gold_star_schema"
        }
      },
      {
        "name": "Trigger_Synapse_DW_Incremental_Refresh",
        "type": "SynapseNotebook",
        "dependsOn": [{ "activity": "Execute_Databricks_Gold_Star_Schema", "dependencyConditions": ["Succeeded"] }]
      }
    ]
  }
}
```

---

## 6. Infrastructure-as-Code (Terraform Blueprint)

To deploy the entire cloud infrastructure deterministically:

```hcl
# main.tf - Enterprise Azure Lakehouse Foundation

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.35.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Resource Group
resource "azurerm_resource_group" "rg" {
  name     = "rg-celebal-retail-prod"
  location = "East US 2"
}

# ADLS Gen2 Storage Account with Hierarchical Namespace
resource "azurerm_storage_account" "lakehouse" {
  name                     = "dlscelebalretailprod"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "ZRS" # Zone-Redundant Storage
  account_kind             = "StorageV2"
  is_hns_enabled           = true  # Enables ADLS Gen2 Hierarchical Namespace
}

# Storage Containers for Medallion Architecture
resource "azurerm_storage_data_lake_gen2_filesystem" "layers" {
  for_each           = toset(["landing", "bronze", "quarantine", "silver", "gold"])
  name               = each.key
  storage_account_id = azurerm_storage_account.lakehouse.id
}

# Azure Databricks Workspace
resource "azurerm_databricks_workspace" "workspace" {
  name                = "dbw-celebal-retail-prod"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "premium"
}

# Azure Key Vault for Enterprise Secret Management
resource "azurerm_key_vault" "kv" {
  name                       = "kv-celebal-retail-prod"
  location                   = azurerm_resource_group.rg.location
  resource_group_name        = azurerm_resource_group.rg.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 90
  purge_protection_enabled   = true
}
```

---

## 7. Cost Optimization & Performance Tuning on Azure

1. **Spot Instances (Single-node / Multi-node Workers):**
   - Use Azure Spot VMs for stateless PySpark worker nodes (saving up to 80% on compute).
   - Keep driver node on On-Demand Standard VM (`Standard_D4ds_v5`) to prevent cluster termination on preemption.

2. **Serverless Databricks SQL Warehouses:**
   - Auto-suspends after 10 minutes of inactivity to eliminate idle compute billing.
   - Photon engine accelerated queries reduce cluster execution runtime by $3\times$.

3. **Storage Lifecycle Policies:**
   - Automatically transition `landing/` and `quarantine/` data older than 90 days to Azure Cool / Archive storage tier.
   - Run daily `VACUUM` with 7-day retention to prevent unreferenced snapshot accumulation.

---

## 8. Summary: Why This Matters for Celebal Technologies

Celebal Technologies is a recognized global leader in **Azure Data & AI** and **Databricks Elite Partner**. Having a pipeline that:
- Natively models Bronze $\to$ Silver $\to$ Gold using open-source Delta Lake standards,
- Employs SCD Type 2 customer history tracking,
- Integrates with Azure Data Factory DAG patterns, and
- Adheres to Unity Catalog three-level naming and Terraform IaC,

demonstrates senior data engineering maturity immediately transferable to Celebal's enterprise client engagements.
