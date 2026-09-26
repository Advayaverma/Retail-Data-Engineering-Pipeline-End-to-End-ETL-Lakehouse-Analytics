# Retail Data Engineering Pipeline — End-to-End Lakehouse Analytics

[![CI/CD Pipeline](https://github.com/placeholder-user/retail-data-engineering/actions/workflows/ci.yml/badge.svg)](https://github.com/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5%2F4.2-red.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2%2F4.4-blue.svg)](https://delta.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg)](https://www.docker.com/)
[![Tests Passing](https://img.shields.io/badge/tests-77%2F77%20passed-brightgreen.svg)](tests/)

A production-grade, portfolio-ready Data Engineering project designed specifically for the **Celebal Technologies Data Engineer — Campus Hiring 2027** evaluation.

---

## 1. Project Overview & Business Problem

Modern omnichannel retail enterprises generate massive, heterogeneous data streams across disparate systems: point-of-sale (POS) transactional feeds, customer relationship management (CRM) databases, product/inventory catalogs, and external macroeconomic REST APIs.

Legacy architectures suffer from the brittle "two-tier" dilemma: unindexed raw data lakes (ADLS/S3) alongside rigid enterprise data warehouses requiring slow, expensive ETL jobs to synchronize.

This project implements an enterprise **Medallion Lakehouse Architecture** (Bronze $\to$ Silver $\to$ Gold) delivering:
- **Resilient Multi-Source Ingestion:** Batch CSVs, PostgreSQL relational extracts, and REST API exchange rates with circuit breakers, exponential backoff retries, and offline fallbacks.
- **Strict Data Quality Gates & Quarantine:** Atomic rule enforcement (`rules.py`) that isolates ~1.05% defective records into a dedicated Quarantine layer with error codes and descriptions, preventing bad data from contaminating downstream analytics.
- **Conformed Silver Cleansing:** Type-safe transformations, timestamp standardization, deduplication, and financial metric derivation.
- **ACID Transactions & Incremental MERGE:** Full Delta Lake commit protocol (`_delta_log/*.json`), schema evolution, and Time Travel audit capabilities.
- **Kimball Dimensional Modeling & SCD Type 2:** Star schema comprising `dim_date`, `dim_product`, `dim_store`, `dim_customer` (Slowly Changing Dimension Type 2 tracking historical vs. active states), and `fact_sales` with surrogate keys.
- **Relational Warehousing & Analytics:** High-performance analytical SQL queries (CTEs, Window functions, dense ranking, customer LTV, and repeat buyer rates) evaluated against a local analytical database and mapped to Azure Synapse / Databricks SQL.
- **Dual Runtime & Azure Cloud Parity:** Runs locally with 100% standard library fallbacks while mapping directly to **Azure Data Factory (ADF)**, **ADLS Gen2**, **Azure Databricks**, and **Unity Catalog**.

---

## 2. End-to-End Medallion Architecture

```
                                  +-------------------------------------------+
                                  |               DATA SOURCES                |
                                  +-------------------------------------------+
                                  |  - Transactions (105k+ CSV records)       |
                                  |  - Customer Master (10.5k Relational DB)  |
                                  |  - Product Catalog (1.2k SKUs)            |
                                  |  - Store Network (55 Branches)            |
                                  |  - Currency Exchange Rates (REST API)     |
                                  +---------------------+---------------------+
                                                        |
                                                        v
                                  +-------------------------------------------+
                                  |             INGESTION ENGINE              |
                                  |   (Python Ingestors + Structured Logger)  |
                                  +---------------------+---------------------+
                                                        |
                                                        v
======================================== MEDALLION LAKEHOUSE ========================================
|                                                                                                  |
|   +------------------------------------------------------------------------------------------+   |
|   |                                       BRONZE LAYER                                       |   |
|   |  - Raw, append-only immutable storage                                                    |   |
|   |  - Lineage audit metadata: _ingested_at, _source_file, _batch_id                         |   |
|   +---------------------------------------------+--------------------------------------------+   |
|                                                 |                                                |
|                                                 v                                                |
|                              [ Data Quality & Quarantine Gate ]                                  |
|                                    /                         \                                   |
|                      (Passed Quality Checks)          (Failed Quality Checks)                    |
|                                   v                               v                              |
|   +-----------------------------------------------+   +--------------------------------------+   |
|   |                 SILVER LAYER                  |   |           QUARANTINE LAYER           |   |
|   |  - Deduplicated & Type-Cast                   |   |  - Isolated Corrupted Transactions   |   |
|   |  - ISO Standardized Timestamps                |   |  - _defect_code & defect_description |   |
|   |  - Derived Financials (gross, discount, net)  |   |  - 1,100 records audited (1.05%)     |   |
|   +-----------------------+-----------------------+   +--------------------------------------+   |
|                           |                                                                      |
|                           v                                                                      |
|           [ Incremental Delta Lake MERGE & Schema Evolution ]                                    |
|                           |                                                                      |
|                           v                                                                      |
|   +------------------------------------------------------------------------------------------+   |
|   |                                        GOLD LAYER                                        |   |
|   |  - Kimball Dimensional Star Schema                                                       |   |
|   |  - dim_customer: SCD Type 2 (10,500 active + 249 expired history versions)               |   |
|   |  - dim_date (2,191 days), dim_product (1,200 SKUs), dim_store (55 branches)              |   |
|   |  - fact_sales: 104,110 transactional fact rows with surrogate keys (sales_sk, etc.)      |   |
|   +---------------------------------------------+--------------------------------------------+   |
|                                                 |                                                |
==================================================|=================================================
                                                  v
                                  +-------------------------------------------+
                                  |               SERVING LAYER               |
                                  |  - Analytical SQL (10 Core Business Qs)   |
                                  |  - Window Functions, CTEs, RFM & CLV      |
                                  |  - Relational DW Engine (retail_dw.db)    |
                                  +-------------------------------------------+
```

---

## 3. Technology Stack & Environment Details

| Technology | Version / Specification | Purpose in Project |
|---|---|---|
| **Python** | `3.11+ / 3.12` | Core pipeline orchestration, ingestion, quality checking, and tests |
| **Apache Spark** | `3.5 / 4.2` | Distributed big data transformations, window aggregation, and joins |
| **Delta Lake** | `3.2 / 4.4` | ACID transaction logging, schema evolution, incremental MERGE, Time Travel |
| **PostgreSQL** | `16-alpine` | Relational source OLTP and staging warehouse |
| **SQLite / SQL** | ANSI / PostgreSQL | Analytical star schema data warehouse with B-tree indexes and window functions |
| **Docker & Compose** | `24.0+` / `v2+` | Multi-container environment (PostgreSQL with healthcheck + headless JRE ETL) |
| **GitHub Actions** | Ubuntu-latest | 4-job CI/CD automation: Linting, Unit/Integration tests, E2E dry-run, Docker build |
| **Databricks** | DBR 14.3+ / Local | 5 Medallion notebooks with dual runtime abstraction (`MockDBUtils`, Volumes) |
| **Microsoft Azure** | Reference Arch | ADF, ADLS Gen2, Azure Databricks, Unity Catalog, Synapse Analytics, Key Vault |

---

## 4. Repository Structure

```text
retail-data-engineering/
├── config/
│   └── pipeline_config.yaml                # Master environment & pipeline configuration
├── data/                                   # Local Lakehouse Storage (Gitignored data)
│   ├── raw/                                # 105k+ transactions, customers, products, stores
│   ├── bronze/                             # Ingested bronze CSVs with lineage metadata
│   ├── quarantine/                         # Isolated defect transactions (1,100 records)
│   ├── silver/                             # Conformed silver tables & delta_transactions
│   ├── gold/                               # Kimball star schema dimensions and fact_sales
│   └── retail_dw.db                        # Analytical SQLite warehouse
├── docker/
│   ├── postgres/init.sql                   # Database initialization DDL & seed schema
│   └── README.md                           # Docker architecture & deployment guide
├── docs/                                   # In-Depth Engineering Documentation
│   ├── source_schemas.md                   # Source schema specifications & data dictionaries
│   ├── indexing_deep_dive.md               # PostgreSQL B-Tree indexing benchmarks
│   ├── data_quality_summary.md             # Quality rules & quarantine defect breakdown
│   ├── pyspark_architecture_and_transformations.md # PySpark execution & broadcast join designs
│   ├── delta_lake_and_incremental_processing.md    # Delta ACID commit logs & Time Travel
│   ├── star_schema_and_scd2_design.md      # Kimball Star Schema & SCD Type 2 architecture
│   ├── business_analytics_report.md        # 10 core business questions executive report
│   ├── databricks_lakehouse_guide.md       # Unity Catalog, workflows & liquid clustering
│   ├── pipeline_execution_summary.md       # Live 7-stage master pipeline execution summary
│   └── azure_cloud_architecture.md         # ADF, ADLS Gen2, Databricks & Terraform IaC
├── notebooks/                              # Production Databricks Notebooks
│   ├── 01_bronze_ingestion.py              # Raw to Bronze ingestion with lineage
│   ├── 02_silver_transformation.py         # Quality validation & quarantine routing
│   ├── 03_gold_star_schema.py              # Kimball dimensional star schema builder
│   ├── 04_data_quality_audit.py            # Quality metrics & SLA compliance audit
│   └── 05_incremental_pipeline.py          # Day-2 Delta MERGE upsert & Time Travel
├── pyspark/                                # PySpark transformation scripts
│   ├── silver/02_bronze_to_silver.py       # Bronze to Silver PySpark transformation
│   ├── gold/03_silver_to_gold.py           # Silver to Gold PySpark business aggregations
│   └── incremental/05_incremental_merge.py # PySpark incremental Delta MERGE script
├── scripts/                                # Master Pipeline & Operational Runners
│   ├── generate_synthetic_data.py          # Seeded (42) synthetic data generator (105k+ rows)
│   ├── run_ingestion.py                    # Multi-source bronze ingestion runner
│   ├── load_postgres.py                    # PostgreSQL source database loader
│   ├── run_data_quality.py                 # Data quality audit & quarantine runner
│   ├── run_transformations.py              # Silver cleansing & Gold aggregates runner
│   ├── run_incremental.py                  # Incremental Delta MERGE & Time Travel runner
│   ├── build_star_schema.py                # Kimball dimensional star schema runner
│   ├── load_gold_dw.py                     # Relational DW loader (retail_dw.db)
│   ├── run_analytics_report.py             # 10 core analytical queries execution runner
│   ├── run_pipeline.py                     # MASTER 7-STAGE DAG PIPELINE ORCHESTRATOR
│   └── mock_api_server.py                  # Resilient mock REST API server
├── sql/                                    # SQL Schemas & Analytics
│   ├── init/                               # Schema creation & index creation DDLs
│   ├── gold/create_star_schema.sql         # Kimball Star Schema DDL
│   └── analytics/                          # Advanced analytical queries (CTEs, Window funcs)
├── src/                                    # Modular Core Python Library
│   ├── ingestion/                          # CSVIngestor, APIIngestor, PostgresIngestor
│   ├── validation/                         # DataQualityChecker, rules.py
│   ├── transformation/                     # SilverTransformer, cleaner.py, delta_lake_engine.py
│   ├── modelling/                          # StarSchemaBuilder, scd2_handler.py, date_dimension.py
│   └── utils/                              # logger.py, databricks_utils.py, spark_session.py
├── tests/                                  # Automated Test Suite (77 Tests)
│   ├── unit/                               # Unit tests covering all components
│   ├── integration/                        # Full end-to-end pipeline integration test
│   └── run_tests.py                        # Master test runner (Zero external dependencies)
├── .github/workflows/ci.yml                # GitHub Actions 4-job CI/CD workflow
├── Dockerfile                              # Multi-stage Docker container (Python 3.11 + JRE 17)
├── docker-compose.yml                      # PostgreSQL + ETL container orchestration
└── requirements.txt                        # Pinned dependencies
```

---

## 5. Quickstart & Execution Guide

### 5.1 Local Quickstart
Run the master pipeline end-to-end in ~28 seconds:
```bash
# 1. Clone repository
git clone https://github.com/placeholder-user/retail-data-engineering.git
cd retail-data-engineering

# 2. Run the entire Master Pipeline (Ingestion -> DQ -> Silver -> Delta -> Gold -> DW -> Analytics)
python scripts/run_pipeline.py

# 3. Run the complete automated test suite (77 tests)
python tests/run_tests.py
```

### 5.2 Docker Quickstart
```bash
# Start PostgreSQL service with healthcheck
docker compose up -d postgres

# Run the complete lakehouse ETL pipeline inside container
docker compose run --rm etl_app

# Run all test assertions inside container
docker compose run --rm etl_app python tests/run_tests.py
```

---

## 6. Implementation Status (All 20 Completed Phases - 100% Complete)

- [x] **Phase 1: Project Foundation** (Modular tree, configuration, Docker setup, CI/CD)
- [x] **Phase 2: Synthetic Data Generation** (105,210 transactions, 10,500 customers, 1,200 products, 55 stores, Day-2 delta feed)
- [x] **Phase 3: Python Ingestion Engine** (Lineage metadata: `_ingested_at`, `_source_file`, `_batch_id`, resilient REST API with retries)
- [x] **Phase 4: PostgreSQL & Indexing** (Relational DDL, B-tree indexes, execution plan benchmarks)
- [x] **Phase 5: Data Quality & Quarantine Framework** (Atomic rule validations, 1,100 defective records quarantined, 98.95% pass rate)
- [x] **Phase 6: PySpark Transformation Engine** (Broadcast joins, window rankings, type-casting, and financial metrics)
- [x] **Phase 7: Delta Lake & Incremental Processing** (ACID `_delta_log/` commits, schema evolution, MERGE upsert, and Time Travel)
- [x] **Phase 8: Dimensional Data Modeling & SCD Type 2** (Kimball Star Schema: `dim_date`, `dim_product`, `dim_store`, `dim_customer` SCD2, `fact_sales`)
- [x] **Phase 9: Business Analytics & SQL Queries** (10 core business queries: MoM growth, Customer LTV, store ranking, repeat buyer rate)
- [x] **Phase 10: Databricks Compatibility** (5 Databricks notebooks, `MockDBUtils` abstraction, Unity Catalog guide)
- [x] **Phase 11: Master Pipeline Orchestrator** (7-stage DAG orchestrator `scripts/run_pipeline.py` with executive reporting)
- [x] **Phase 12: Docker Containerization** (Multi-service Docker Compose with PostgreSQL healthcheck)
- [x] **Phase 13: End-to-End Integration Testing** (Comprehensive assertions verifying data integrity across all medallion layers)
- [x] **Phase 14: GitHub Actions CI/CD** (4-stage workflow: Linting, Unit/Integration tests, E2E dry-run, Docker image build)
- [x] **Phase 15: Azure Cloud Architecture Extension** (Enterprise mapping to ADF, ADLS Gen2, Azure Databricks, and Terraform IaC)
- [x] **Phase 16: Comprehensive Portfolio README** (Executive problem statement, architecture diagrams, benchmark summaries)
- [x] **Phase 17: Visual Architecture Diagrams** (Mermaid medallion lineage, Kimball star schema ERD, SCD2 flow, Azure topology)
- [x] **Phase 18: Code Quality Audit & Polish** (Zero plaintext credentials, clean `.gitignore`, 77/77 tests passing)
- [x] **Phase 19: Celebal Technologies Interview Preparation Guide** (Technical & behavioral Q&A, STAR stories, exact metrics)
- [x] **Phase 20: ATS-Optimized Resume Bullets** (Metric-driven bullets, LinkedIn project summary, ATS keywords)
