# Databricks Lakehouse Architecture & Production Deployment Guide
**Target Role:** Celebal Technologies Data Engineer Evaluation  
**Lakehouse Paradigm:** Delta Lake & Medallion Architecture on Azure Databricks

---

## 1. Executive Summary & Databricks Value Proposition
In modern cloud analytics, legacy architectures suffer from the "two-tier" dilemma: a raw data lake (blob/ADLS) for cheap unstructured storage alongside a rigid enterprise data warehouse (EDW) requiring fragile ETL pipelines to sync.

The **Databricks Lakehouse** unifies these tiers by layering **ACID transactional guarantees**, schema enforcement, and indexing directly over object storage via open-standard **Delta Lake**. In this repository, the entire end-to-end pipeline is engineered with dual runtime compatibility: it executes natively on local developer machines while seamlessly running on Databricks clusters with zero code refactoring.

```
       +--------------------------------------------------------------+
       |                Azure Databricks Lakehouse                    |
       |                                                              |
       |  +--------------------+   +-------------------------------+  |
       |  | 01_bronze_ingest   |-->| 02_silver_transformation      |  |
       |  | (Raw Append-Only)  |   | (DQ Quarantine & Cleanse)     |  |
       |  +--------------------+   +---------------+---------------+  |
       |                                           |                  |
       |                                           v                  |
       |  +--------------------+   +---------------+---------------+  |
       |  | 05_incremental_mrg |<--| 03_gold_star_schema           |  |
       |  | (Delta ACID MERGE) |   | (Kimball Facts & SCD Type 2)  |  |
       |  +--------------------+   +-------------------------------+  |
       |                                                              |
       +--------------------------------------------------------------+
```

---

## 2. Medallion Pipeline Notebook Organization

All notebooks in `notebooks/` are formatted in standard Databricks format (`# Databricks notebook source` and `# COMMAND ----------` cells), making them instantly importable via Databricks Repos or CLI:

1. **`01_bronze_ingestion.py`**:
   - Ingests landed transactional feeds from ADLS Gen2 (`/Volumes/retail_catalog/landing`).
   - Appends lineage audit columns: `_ingested_at`, `_source_file`, `_batch_id`.
   - Stores raw payloads into Bronze Delta tables with append-only integrity.

2. **`02_silver_transformation.py`**:
   - Executes atomic data quality validation rules (`rules.py`).
   - Routes defective transactions (~1.36% corrupted records) directly to `data/quarantine/`.
   - Cleanses timestamps, parses product codes, calculates line item totals, and deduplicates.

3. **`03_gold_star_schema.py`**:
   - Builds high-performance dimensional models (Kimball Star Schema).
   - Generates surrogate keys (`customer_sk`, `product_sk`, `store_sk`, `date_sk`).
   - Implements **SCD Type 2** for customer dimension (maintaining active records with `is_current = TRUE` and historical records with valid date ranges).
   - Assembles `fact_sales` aligned to business grain (`transaction_id` + line item).

4. **`04_data_quality_audit.py`**:
   - Analyzes quarantine distribution across error codes (`ERR_NULL_TX_ID`, `ERR_QTY_OUT_OF_RANGE`, etc.).
   - Computes SLA pass rate (98.95%) and defect rate (1.05%).

5. **`05_incremental_pipeline.py`**:
   - Reads incoming Day-2 delta feed (5,000 new records + 500 existing record updates).
   - Executes ACID atomic `MERGE` using Delta Lake protocol.
   - Demonstrates **Time Travel** capability by querying snapshot `VERSION AS OF 0` alongside `VERSION AS OF 1`.

---

## 3. Unity Catalog Governance & Data Lineage

On Databricks, Unity Catalog provides centralized governance across workspaces:

```sql
-- 1. Create Enterprise Catalog & Schemas
CREATE CATALOG IF NOT EXISTS retail_catalog;
USE CATALOG retail_catalog;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 2. Define External Volume for Landing Zone
CREATE EXTERNAL VOLUME IF NOT EXISTS retail_catalog.bronze.landing_volume
LOCATION 'abfss://landing@retaildatalake.dfs.core.windows.net/';

-- 3. Create Managed Bronze Table with Delta Lake Protocol
CREATE TABLE IF NOT EXISTS retail_catalog.bronze.transactions_raw (
    transaction_id STRING,
    customer_id STRING,
    product_id STRING,
    store_id STRING,
    quantity INT,
    unit_price DOUBLE,
    total_amount DOUBLE,
    payment_method STRING,
    transaction_timestamp STRING,
    _ingested_at TIMESTAMP,
    _source_file STRING,
    _batch_id STRING
) USING DELTA;
```

---

## 4. Multi-Task Workflow Orchestration (Databricks Workflows)

In production, individual notebooks are orchestrated as a DAG using **Databricks Workflows**:

```
[01_bronze_ingestion]
         |
         v
[02_silver_transformation]  <---> [04_data_quality_audit]
         |
         v
[03_gold_star_schema]
         |
         v
[05_incremental_pipeline]
```

### Databricks Job Specification (`job_spec.json`):
```json
{
  "name": "Retail_Lakehouse_Daily_ETL",
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "notebook_task": {
        "notebook_path": "/Repos/production/retail-pipeline/notebooks/01_bronze_ingestion"
      },
      "job_cluster_key": "retail_cluster"
    },
    {
      "task_key": "silver_transformation",
      "depends_on": [{"task_key": "bronze_ingestion"}],
      "notebook_task": {
        "notebook_path": "/Repos/production/retail-pipeline/notebooks/02_silver_transformation"
      },
      "job_cluster_key": "retail_cluster"
    },
    {
      "task_key": "gold_star_schema",
      "depends_on": [{"task_key": "silver_transformation"}],
      "notebook_task": {
        "notebook_path": "/Repos/production/retail-pipeline/notebooks/03_gold_star_schema"
      },
      "job_cluster_key": "retail_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "retail_cluster",
      "new_cluster": {
        "spark_version": "14.3.x-scala2.12",
        "node_type_id": "Standard_D4ds_v5",
        "num_workers": 2,
        "spark_conf": {
          "spark.databricks.delta.preview.enabled": "true"
        }
      }
    }
  ]
}
```

---

## 5. Performance Optimizations in Databricks

### 5.1 Liquid Clustering vs. Z-Ordering
Traditional table partitioning (`PARTITIONED BY (store_id)`) often leads to the **small file problem** when high-cardinality keys are used.
In Databricks Runtime 13.3+, **Liquid Clustering** replaces traditional partitioning:
```sql
-- Liquid Clustering dynamically optimizes layout without data skew
ALTER TABLE retail_catalog.silver.transactions_silver
CLUSTER BY (store_id, transaction_timestamp);
```

### 5.2 Auto-Compaction & Optimized Writes
To prevent small file fragmentation during incremental micro-batches:
```sql
ALTER TABLE retail_catalog.gold.fact_sales SET TBLPROPERTIES (
   'delta.autoOptimize.optimizeWrite' = 'true',
   'delta.autoOptimize.autoCompact' = 'true'
);
```

### 5.3 Vacuum & Retention Policy
```sql
-- Maintain 7 days of audit/time travel retention
SET spark.databricks.delta.vacuum.parallelDelete.enabled = true;
VACUUM retail_catalog.gold.fact_sales RETAIN 168 HOURS;
```

---

## 6. How to Test & Verify Locally
This repository includes a dedicated test runner `tests/unit/test_databricks_notebooks.py` that verifies:
- All 5 notebooks execute successfully under the local abstraction layer.
- `MockDBUtils` resolves mock filesystem and widget operations without errors.
- Dual-runtime detection (`is_running_on_databricks()`) behaves reliably.
