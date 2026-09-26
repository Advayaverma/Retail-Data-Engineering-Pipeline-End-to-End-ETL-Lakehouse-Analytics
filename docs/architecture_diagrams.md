# Enterprise Lakehouse Architecture & Lineage Diagrams
**Project:** Retail Data Engineering Pipeline — End-to-End Lakehouse Analytics  
**Target Role:** Celebal Technologies Data Engineer Evaluation  
**Diagram Formats:** Mermaid.js & ASCII Diagrams for Technical Presentations & Architecture Reviews

---

## 1. End-to-End Lakehouse Medallion Pipeline (Mermaid)

```mermaid
flowchart TD
    subgraph Sources["1. Multi-Source Ingestion Layer"]
        S1["POS Transactions CSV<br/>(105,210 rows)"]
        S2["Customer Master DB<br/>(10,500 records)"]
        S3["Product Catalog CSV<br/>(1,200 SKUs)"]
        S4["Store Network CSV<br/>(55 branches)"]
        S5["Forex Rates REST API<br/>(USD conversions)"]
    end

    subgraph Bronze["2. Bronze Layer (Raw Storage & Lineage)"]
        B1[("bronze_transactions<br/>+ _ingested_at<br/>+ _source_file<br/>+ _batch_id")]
        B2[("bronze_customers")]
        B3[("bronze_products")]
        B4[("bronze_stores")]
    end

    subgraph Quality["3. Data Quality & Quarantine Gate"]
        DQ{"Atomic Rules Engine<br/>rules.py"}
        Q1[("quarantine_transactions<br/>1,100 records (1.05%)<br/>_defect_code, _description")]
    end

    subgraph Silver["4. Silver Layer (Cleaned & Conformed)"]
        SV1[("silver_transactions<br/>Deduplicated & Cleaned<br/>(104,110 rows)")]
        SV2[("delta_transactions<br/>ACID MERGE / Versioning<br/>Time Travel History")]
    end

    subgraph Gold["5. Gold Layer (Kimball Dimensional Star Schema)"]
        D_DATE[("dim_date<br/>2,191 dates")]
        D_CUST[("dim_customer<br/>SCD Type 2 History<br/>10,749 records")]
        D_PROD[("dim_product<br/>1,200 SKUs")]
        D_STORE[("dim_store<br/>55 branches")]
        F_SALES[("fact_sales<br/>104,110 rows<br/>Surrogate Keys")]
    end

    subgraph Serving["6. Analytical Serving & Insights"]
        DW[("retail_dw.db<br/>Relational DW")]
        REP["10 Core SQL Analytics<br/>MoM Growth, CLV, RFM"]
    end

    S1 --> B1
    S2 --> B2
    S3 --> B3
    S4 --> B4
    S5 -.-> B1

    B1 --> DQ
    DQ -- "Fail (1.05%)" --> Q1
    DQ -- "Pass (98.95%)" --> SV1

    SV1 --> SV2
    SV1 --> F_SALES
    B2 --> D_CUST
    B3 --> D_PROD
    B4 --> D_STORE

    D_DATE --> F_SALES
    D_CUST --> F_SALES
    D_PROD --> F_SALES
    D_STORE --> F_SALES

    F_SALES --> DW
    DW --> REP
```

---

## 2. Kimball Star Schema Entity-Relationship Diagram (ERD)

```mermaid
erDiagram
    fact_sales {
        INTEGER sales_sk PK
        INTEGER date_sk FK
        INTEGER customer_sk FK
        INTEGER product_sk FK
        INTEGER store_sk FK
        VARCHAR transaction_id
        INTEGER quantity
        NUMERIC unit_price
        NUMERIC discount
        NUMERIC gross_amount
        NUMERIC discount_amount
        NUMERIC net_sales
        NUMERIC total_cost
        NUMERIC profit
        NUMERIC profit_margin_pct
        VARCHAR payment_method
    }

    dim_date {
        INTEGER date_sk PK
        VARCHAR full_date
        INTEGER day_of_week
        VARCHAR day_name
        INTEGER day_of_month
        INTEGER day_of_year
        INTEGER week_of_year
        INTEGER month_num
        VARCHAR month_name
        INTEGER quarter_num
        VARCHAR quarter_name
        INTEGER year_num
        BOOLEAN is_weekend
        VARCHAR fiscal_quarter
        INTEGER fiscal_year
    }

    dim_customer {
        INTEGER customer_sk PK
        VARCHAR customer_id
        VARCHAR customer_name
        VARCHAR email
        VARCHAR city
        VARCHAR state
        VARCHAR customer_segment
        VARCHAR effective_from
        VARCHAR effective_to
        BOOLEAN is_current
        INTEGER version
    }

    dim_product {
        INTEGER product_sk PK
        VARCHAR product_id
        VARCHAR product_name
        VARCHAR category
        VARCHAR subcategory
        VARCHAR brand
        NUMERIC unit_cost
        NUMERIC recommended_price
    }

    dim_store {
        INTEGER store_sk PK
        VARCHAR store_id
        VARCHAR store_name
        VARCHAR city
        VARCHAR state
        VARCHAR region
        VARCHAR store_type
        VARCHAR opened_date
    }

    dim_date ||--o{ fact_sales : "validates on date_sk"
    dim_customer ||--o{ fact_sales : "links via customer_sk (SCD2)"
    dim_product ||--o{ fact_sales : "links via product_sk"
    dim_store ||--o{ fact_sales : "links via store_sk"
```

---

## 3. Slowly Changing Dimensions (SCD Type 2) Lifecycle Flow

```mermaid
sequenceDiagram
    autonumber
    participant Src as Source Feeds
    participant SCD2 as SCD2 Handler
    participant DimCust as dim_customer Table

    Note over Src,DimCust: Initial Load (Day 1)
    Src->>SCD2: Ingest 10,500 Customer Master Records
    SCD2->>DimCust: Insert Version 1 (is_current=TRUE, effective_from=2024-01-01, effective_to=9999-12-31)

    Note over Src,DimCust: Day 2 Incremental Updates (250 changes detected)
    Src->>SCD2: Ingest Day 2 Customer Change Feed
    SCD2->>SCD2: Compare Hashing of Tracking Attributes (Segment, City, State)
    SCD2->>DimCust: Expire Existing Record (is_current=FALSE, effective_to=2026-01-01)
    SCD2->>DimCust: Insert New Version 2 (is_current=TRUE, effective_from=2026-01-01, effective_to=9999-12-31)
```

---

## 4. Azure Enterprise Production Architecture

```text
       +-----------------------------------------------------------------------------------------+
       |                                Microsoft Azure Cloud                                    |
       |                                                                                         |
       |  +-----------------------------+         +--------------------------------------------+ |
       |  |     Azure Data Factory      |         |     Azure Data Lake Storage Gen2 (ADLS)    | |
       |  |  (Pipelines & Triggers)     |-------->|  - abfss://landing@retaildls.dfs.core/     | |
       |  +--------------+--------------+         |  - abfss://bronze@retaildls.dfs.core/      | |
       |                 |                        |  - abfss://silver@retaildls.dfs.core/      | |
       |                 | (Orchestrates DAG)     |  - abfss://gold@retaildls.dfs.core/        | |
       |                 v                        +---------------------+----------------------+ |
       |  +-------------------------------------+                       |                        |
       |  |         Azure Databricks            |<----------------------+ (Reads/Writes Delta)   |
       |  |  - Unity Catalog Governance         |                                                |
       |  |  - Multi-task Notebook Workflows    |                                                |
       |  |  - Liquid Clustering (Opt. Storage) |                                                |
       |  +--------------+----------------------+                                                |
       |                 |                                                                       |
       |                 v                                                                       |
       |  +-------------------------------------+         +------------------------------------+ |
       |  |   Azure Synapse / Fabric Warehouse  |         |        Power BI DirectLake         | |
       |  |   (MPP Relational Serving Layer)    |-------->|   (Executive KPIs & Analytics)     | |
       |  +-------------------------------------+         +------------------------------------+ |
       |                                                                                         |
       |  Cross-Cutting Security & Monitoring:                                                   |
       |  - Azure Key Vault (Managed Identities & Zero Secret Storage)                           |
       |  - Azure Monitor & Log Analytics (KQL Queries & Alerts)                                 |
       |  - Microsoft Entra ID (Role-Based Access Control - RBAC)                                |
       +-----------------------------------------------------------------------------------------+
```
