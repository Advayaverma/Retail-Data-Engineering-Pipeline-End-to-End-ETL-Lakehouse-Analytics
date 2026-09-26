# Retail Data Engineering Pipeline — End-to-End ETL & Lakehouse Analytics

[![CI/CD Pipeline](https://github.com/placeholder-user/retail-data-engineering/actions/workflows/ci.yml/badge.svg)](https://github.com/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-red.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.2-blue.svg)](https://delta.io/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue.svg)](https://www.postgresql.org/)
[![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg)](https://www.docker.com/)

A production-grade, portfolio-ready Data Engineering project designed specifically for the **Celebal Technologies Data Engineer — Campus Hiring 2027** evaluation.

---

## 1. Project Overview & Business Problem

Modern retail organizations ingest multi-modal data from varied channels: point-of-sale transactional streams, customer relationship platforms (CRM), inventory catalog systems, and external macroeconomic REST APIs.

This project implements an enterprise **Medallion Lakehouse Architecture** (Bronze $\to$ Silver $\to$ Gold) utilizing **PySpark**, **Delta Lake**, **PostgreSQL**, and **Docker**.

### Key Objectives:
- Ingest high-volume retail transactions, relational customer profiles, and foreign exchange rates via REST API.
- Enforce strict Data Quality (DQ) validation gates, isolating defective records into a dedicated **Quarantine Layer**.
- Transform raw bronze payloads into cleaned, deduplicated, and normalized **Silver Delta Tables**.
- Model a Kimball **Dimensional Star Schema** in the **Gold Layer**, implementing **Slowly Changing Dimensions (SCD Type 2)** for historical customer tracking.
- Demonstrate true **Incremental Processing** with Delta Lake `MERGE` (upserting Day-2 delta changes rather than full reloads).
- Serve analytical business queries using SQL window functions, CTEs, and aggregations.

---

## 2. Medallion Architecture

```text
               +-------------------------------------------+
               |               DATA SOURCES                |
               +-------------------------------------------+
               |  - Transactions (CSV, 100k+ records)      |
               |  - Customer Master (PostgreSQL Relational)|
               |  - Product & Store Catalogs (CSV)         |
               |  - Currency Exchange Rates (REST API)     |
               +---------------------+---------------------+
                                     |
                                     v
               +-------------------------------------------+
               |             INGESTION LAYER               |
               |     (Python Ingestors & Logging)          |
               +---------------------+---------------------+
                                     |
                                     v
============================== MEDALLION LAKEHOUSE ==============================
|                                                                                |
|   +------------------------------------------------------------------------+   |
|   |                              BRONZE LAYER                              |   |
|   |  - Raw, immutable append-only storage in Delta Lake format             |   |
|   |  - Preserves technical metadata: _ingested_at, _source_file, _batch_id |   |
|   +-----------------------------------+------------------------------------+   |
|                                       |                                        |
|                                       v                                        |
|                    [ PySpark Data Quality Validator ]                          |
|                          /                         \                           |
|      (Passed Quality Gates)                         (Failed Quality Gates)     |
|                         v                             v                        |
|   +------------------------------------+   +-------------------------------+   |
|   |            SILVER LAYER            |   |       QUARANTINE LAYER        |   |
|   |  - Deduplicated & Type-Cast        |   |  - Schema/Referential errors  |   |
|   |  - Standardized Timestamps         |   |  - Bad values & audit reasons |   |
|   |  - Currency Normalized to USD      |   |  - Isolated for root cause    |   |
|   +-----------------+------------------+   +-------------------------------+   |
|                     |                                                          |
|                     v                                                          |
|          [ Dimensional Modeling & Incremental MERGE (SCD Type 2) ]             |
|                     |                                                          |
|                     v                                                          |
|   +------------------------------------------------------------------------+   |
|   |                               GOLD LAYER                               |   |
|   |  - Fact: fact_sales (grain: line item per transaction)                 |   |
|   |  - Dims: dim_customer (SCD Type 2), dim_product, dim_store, dim_date   |   |
|   |  - Aggregates: monthly_sales_summary, customer_rfm_metrics             |   |
|   +-----------------------------------+------------------------------------+   |
|                                       |                                        |
========================================|=========================================
                                        v
               +-------------------------------------------+
               |               SERVING LAYER               |
               |  - Analytical SQL (CTEs, Window Funcs)    |
               |  - PostgreSQL Gold Analytics Tables       |
               +-------------------------------------------+
```

---

## 3. Technology Stack

- **Core Engine:** Apache Spark 3.5 / PySpark
- **Storage & Lakehouse:** Delta Lake 3.2 (ACID, Schema Enforcement, Time Travel, Upsert MERGE)
- **Programming & Scripting:** Python 3.11+, SQL (ANSI / PostgreSQL dialect)
- **Relational Database:** PostgreSQL 16 (Source OLTP & Gold Warehouse Serving)
- **Containerization:** Docker & Docker Compose
- **Quality & Testing:** Pytest, Flake8, Custom Data Quality & Quarantine Framework
- **CI/CD:** GitHub Actions (Linting, Test Execution, Docker Container Build)
- **Enterprise / Cloud Alignment:** Databricks Notebooks & Azure Data Factory / ADLS Gen2 Mapping

---

## 4. Repository Structure

```text
retail-data-engineering/
├── data/                       # Medallion data lakehouse layers (gitignored)
│   ├── raw/                    # Raw generated datasets & API responses
│   ├── bronze/                 # Raw ingested Delta tables with audit fields
│   ├── silver/                 # Cleansed, validated, normalized Delta tables
│   ├── gold/                   # Analytical Star Schema Delta tables
│   └── quarantine/             # Rejected bad records with failure reasons
├── src/                        # Modular Python source library
│   ├── ingestion/              # CSV, PostgreSQL, and REST API extractors
│   ├── transformation/         # Cleansing, type-casting, currency normalization
│   ├── validation/             # Rule-based data quality engine & quarantine router
│   ├── modelling/              # Kimball star schema builders & SCD Type 2 logic
│   └── utils/                  # DB connections, logging, Spark session factory
├── pyspark/                    # Standalone PySpark pipeline jobs
│   ├── bronze/                 # Raw to Bronze Delta ingestion
│   ├── silver/                 # Bronze to Silver cleaning & validation
│   ├── gold/                   # Silver to Gold dimensional modeling
│   └── incremental/            # Day-2 Delta MERGE upsert pipeline
├── sql/                        # SQL DDL, staging, and analytical queries
│   ├── init/                   # PostgreSQL DDL and schema init
│   ├── staging/                # Operational staging queries
│   └── analytics/              # MoM growth, Customer LTV, RFM, Top products
├── notebooks/                  # Databricks-compatible notebooks
├── tests/                      # Automated unit and integration test suite
├── config/                     # Pipeline YAML configurations
├── scripts/                    # Operational run scripts & synthetic data generator
├── docker/                     # Docker setup files
├── .github/workflows/          # CI/CD GitHub Actions workflow
├── requirements.txt            # Python dependencies
├── Dockerfile                  # Multi-stage Docker container
└── docker-compose.yml          # Container orchestration for Postgres & ETL
```

---

## 5. Current Implementation Status

- [x] **Phase 1: Project Foundation** (Folder structure, configuration, Docker setup, CI/CD pipeline)
- [ ] **Phase 2: Synthetic Data Generation** (100k+ transactions, customers, products, stores with controlled defects)
- [ ] **Phase 3: Python Ingestion Engine** (CSV, PostgreSQL, and REST API extractors)
- [ ] **Phase 4: PostgreSQL Setup** (Dockerized relational database, schemas, indexes, and queries)
- [ ] **Phase 5: Data Quality & Quarantine Framework** (Configurable checks, rejection logging, audit reporting)
- [ ] **Phase 6: PySpark Transformation Engine** (Bronze to Silver cleaning, Silver to Gold joins)
- [ ] **Phase 7: Delta Lake & Incremental Processing** (ACID transactions, Time Travel, Delta `MERGE`)
- [ ] **Phase 8: Dimensional Data Modeling** (Kimball Star Schema, Surrogate Keys, SCD Type 2)
- [ ] **Phase 9: Business Analytics & SQL Queries** (10 business questions, Window functions, CTEs)
- [ ] **Phase 10: Databricks Compatibility** (Production notebook scripts, cluster configurations)
- [ ] **Phase 11: Pipeline Orchestration** (Master execution engine with dependency management)
- [ ] **Phase 12: Docker Containerization** (Full containerized execution)
- [ ] **Phase 13: Automated Testing Suite** (Unit & integration tests)
- [ ] **Phase 14: CI/CD Automation** (GitHub Actions verification)
- [ ] **Phase 15: Azure Cloud Architecture Extension** (ADLS Gen2, ADF, Databricks mapping)
- [ ] **Phase 16: Comprehensive Documentation**
- [ ] **Phase 17: Visual Architecture Diagrams**
- [ ] **Phase 18: Code Audit & Polish**
- [ ] **Phase 19: Celebal Technologies Interview Preparation Guide**
- [ ] **Phase 20: ATS-Optimized Resume Bullets**
