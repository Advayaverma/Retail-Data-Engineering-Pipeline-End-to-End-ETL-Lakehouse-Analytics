# Celebal Technologies Technical & Behavioral Interview Preparation Guide
**Position:** Data Engineer (Campus Hiring 2027 / Lateral Hire)  
**Company Profile:** Celebal Technologies (Premier Microsoft Gold Partner & Databricks Elite Partner)  
**Project Focus:** End-to-End Retail Data Lakehouse & Cloud Analytics Architecture

---

## 1. Company Context: What Celebal Technologies Looks For

Celebal Technologies specializes in enterprise digital transformation across **Microsoft Azure**, **Databricks**, **Power BI**, and **SAP/ERP Integrations**. When evaluating a Data Engineer candidate, interviewers focus heavily on:
1. **Medallion Lakehouse Mastery:** Practical understanding of why and how data transitions through Bronze $\to$ Silver $\to$ Gold.
2. **Delta Lake & ACID Internals:** Transaction logs, concurrency control, Time Travel, Liquid Clustering, and incremental upserts (`MERGE`).
3. **Data Quality & Quarantine Patterns:** How production systems isolate bad data without failing the whole batch.
4. **Data Modeling (Kimball Star Schema & SCDs):** Translating business logic into performant facts and slowly changing dimensions (especially SCD Type 2).
5. **PySpark & SQL Performance Tuning:** Broadcast joins, shuffle partitions, predicate pushdown, and B-Tree indexing.
6. **Azure Cloud Architecture:** Designing scalable solutions on ADLS Gen2, Azure Data Factory (ADF), Azure Databricks, and Synapse.

---

## 2. Core Technical Questions & High-Impact Model Answers

### Q1: "Can you walk me through the architecture of your retail data engineering project?"
> **Model Answer:**
> "I engineered an end-to-end retail lakehouse pipeline designed around the Medallion Architecture. 
> 
> - **Ingestion:** We ingest multi-source data: 105k+ POS transactions, customer records from a relational database, product/store master catalogs, and currency exchange rates via a REST API with exponential backoff retries. Every raw record is stamped with technical metadata (`_ingested_at`, `_source_file`, `_batch_id`) into an append-only **Bronze** layer.
> - **Data Quality & Quarantine Gate:** Before moving to Silver, records pass through an atomic rule engine (`rules.py`). Non-compliant records (~1.05% of rows, including missing customer IDs, invalid price ranges, and timestamp corruptions) are segregated into a **Quarantine layer** with defect codes (`_defect_code`).
> - **Silver Layer:** Cleansed records are deduplicated, timestamps normalized to ISO format, and derived fields computed. We use **Delta Lake ACID transactions** to handle incremental Day-2 change data feeds via `MERGE` upsert and demonstrate Time Travel.
> - **Gold Layer:** We model a **Kimball Star Schema** containing surrogate-keyed dimension tables (`dim_date`, `dim_product`, `dim_store`) and implement **SCD Type 2** on `dim_customer` to track demographic/segment changes with active vs. expired flags. The grain of `fact_sales` is aligned to transaction line items.
> - **Serving & Analytics:** Finally, data is loaded into a relational warehouse (`retail_dw.db`) where 10 complex analytical queries (CTEs, Window functions, CLV, RFM) generate executive reporting. The entire pipeline is orchestrated via a 7-stage master DAG, containerized with Docker, covered by 77 automated tests, and mapped directly to Azure (ADF, ADLS Gen2, Azure Databricks)."

---

### Q2: "Why did you choose Delta Lake over standard Parquet in your data lakehouse?"
> **Model Answer:**
> "While Parquet is an outstanding columnar file format, it lacks transactional guarantees. In an enterprise retail environment, standard Parquet suffers from three major flaws:
> 1. **No ACID Guarantees:** A failed pipeline run mid-write leaves orphaned files, resulting in dirty reads or partial writes. Delta Lake solves this with its **ACID commit log** (`_delta_log/*.json`), making every commit atomic via WriteSerializable isolation.
> 2. **Lack of In-Place Upserts (`MERGE`):** Updating a customer profile or adjusting a returned order in raw Parquet requires rewriting the entire partition. Delta Lake provides native `MERGE INTO`, isolating changed rows and rewriting only affected data files.
> 3. **Schema Enforcement & Evolution:** Delta Lake prevents bad schema pollution by failing when unexpected columns arrive unless explicitly overridden with `mergeSchema=True`.
> 4. **Time Travel:** Delta Lake allows auditing and rolling back data to any historical version using `VERSION AS OF` or `TIMESTAMP AS OF`."

---

### Q3: "How did you implement SCD Type 2 for the Customer dimension, and why?"
> **Model Answer:**
> "Customer attributes such as city, state, and `customer_segment` (e.g., Standard vs. VIP) change over time. If we simply overwrite them (SCD Type 1), historical sales made under the previous segment or region would be misattributed in historical reporting.
> 
> To preserve historical accuracy, I implemented **SCD Type 2**:
> - Each customer record receives a synthetic surrogate key (`customer_sk`), an `effective_from` date, an `effective_to` date, an `is_current` boolean flag, and a `version` integer.
> - When a customer record updates in the Day-2 batch, the engine identifies the record, expires the active record by setting `is_current = FALSE` and `effective_to = current_timestamp`, and inserts a new row with `version = version + 1`, `is_current = TRUE`, and `effective_to = '9999-12-31'`.
> - In our dataset, this maintained 10,500 currently active customer records alongside 249 historical records, enabling exact point-in-time joins for `fact_sales`."

---

### Q4: "How does your pipeline handle bad or corrupted data without crashing?"
> **Model Answer:**
> "Rather than using a naive `try/except` that either drops records silently or aborts the entire 100k-row batch, I engineered a dedicated **Data Quality and Quarantine Router**:
> - I established atomic validation rules implementing `BaseRule`: null checks on primary keys, numeric range boundaries (e.g., $0.01 \le \text{price} \le \$50,000$), regex timestamp format parsing, referential integrity checks against customer and product masters, and duplicate tracking.
> - Records failing any validation rule are rejected and routed to `data/quarantine/transactions_quarantine.csv`.
> - Each quarantined record is enriched with `_defect_code` (e.g., `ERR_MISSING_CUSTOMER_ID`, `ERR_REF_CUSTOMER`), `_defect_description`, and `_quarantined_at`.
> - Clean records (98.95% pass rate) proceed smoothly to Silver, while data engineers and downstream teams can inspect the quarantine dashboard to identify upstream anomalies without interrupting downstream SLA timelines."

---

### Q5: "How would you optimize PySpark jobs if you observed severe data skew or out-of-memory (OOM) errors?"
> **Model Answer:**
> "There are several targeted techniques I would employ:
> 1. **Broadcast Hash Joins:** When joining a large fact table (e.g., 100k+ transactions) with small lookup dimension tables (e.g., 55 stores or 1,200 products), standard shuffle hash joins cause expensive cross-network data movement. Using `broadcast(dim_df)` copies the dimension table to all executors, eliminating the shuffle phase entirely.
> 2. **Salting for Skewed Keys:** If a single high-volume store (e.g., Black Friday flagship store) causes all records to concentrate on one executor partition, I would append a random salt integer (e.g., `0` to `N-1`) to the join key on the fact side and replicate the dimension keys by the same factor, spreading the load evenly across partitions.
> 3. **Tuning `spark.sql.shuffle.partitions`:** The default 200 partitions is often excessive for small-to-medium batches (creating tiny files) and insufficient for terabyte-scale jobs. I right-size this based on 100–200MB target partition size.
> 4. **Liquid Clustering in Databricks:** In modern Databricks runtimes, I use Liquid Clustering (`CLUSTER BY (store_id, transaction_timestamp)`) instead of traditional Hive partitioning, which avoids small file fragmentation and handles data skew dynamically."

---

### Q6: "How do you translate this project to an enterprise Microsoft Azure deployment?"
> **Model Answer:**
> "Every component in this project was architected to map 1:1 with Microsoft Azure services:
> - **Landing & Medallion Storage:** Maps directly to **Azure Data Lake Storage Gen2 (ADLS)** using Hierarchical Namespaces with separate containers (`landing`, `bronze`, `quarantine`, `silver`, `gold`).
> - **Compute & Transformation:** Runs in **Azure Databricks** with **Unity Catalog** managing three-level namespaces (`retail_catalog.silver.transactions_silver`).
> - **Orchestration:** Managed via **Azure Data Factory (ADF)** or **Databricks Workflows** as a multi-task DAG with webhook alerts and failure retries.
> - **Serving:** Star schema gold tables are served through **Azure Synapse Dedicated SQL Pool** or queried directly in Power BI via **DirectLake mode**.
> - **Security & CI/CD:** Credentials managed via **Azure Key Vault** with Managed Identities (`DefaultAzureCredential`), and deployments automated via **GitHub Actions** and **Terraform**."

---

## 3. Behavioral & Project Ownership Questions (STAR Method)

### Question: "Describe a technical challenge you encountered while building this pipeline and how you resolved it."
- **Situation:** "While engineering the incremental processing phase, I needed to implement an ACID-compliant Delta Lake MERGE operation locally that replicated Databricks' Delta engine behavior without requiring an active cloud cluster for development testing."
- **Task:** "I needed to implement transaction log serialization, schema evolution support, and Time Travel versioning using Python standard libraries while maintaining exact compatibility with Delta Lake protocols."
- **Action:** "I created `DeltaLakeTable` which writes atomic JSON commit logs under `_delta_log/` matching Delta protocol specifications. When the Day-2 delta feed introduced a new `loyalty_points_earned` column, I implemented a schema enforcement checker that dynamically evolves the table schema when `merge_schema=True` is provided, while updating modified records and inserting new transactions."
- **Result:** "The solution successfully committed Day 1 baseline (104,110 rows, v0), executed Day 2 MERGE (500 updated, 5,000 inserted, v1), and enabled querying `read_version(0)` vs `read_version(1)` with 100% data fidelity, covered by automated integration tests."

---

## 4. Key Metrics to Mention in Interviews

Memorize these exact figures from your pipeline run to demonstrate authentic project ownership:
- **Total Ingested Transactions:** 105,210 rows across 24 calendar months.
- **Data Quality SLA Pass Rate:** **98.95%** (104,110 clean rows).
- **Quarantined Defects:** **1,100 rows (1.05%)** categorized across 6 error codes (`ERR_MISSING_CUSTOMER_ID`: 253, `ERR_REF_CUSTOMER`: 220, `ERR_DUPLICATE_RECORD`: 210, `ERR_INVALID_PRICE`: 155, `ERR_INVALID_QUANTITY`: 141, `ERR_MALFORMED_TIMESTAMP`: 121).
- **Customer Dimension (SCD2):** 10,500 active customers (`is_current=True`) + 249 historical records (`is_current=False`).
- **Product SKUs & Stores:** 1,200 unique products across 55 retail branches.
- **Financial Analytics Results:** **$76.22M Total Net Revenue**, **$20.73M Gross Profit (27.2% margin)**.
- **Repeat Buyer Rate:** **99.96%**.
- **Top Revenue Region:** South ($24.34M, 31.94% share).
- **Full Pipeline Execution Time:** **~26.7 seconds** end-to-end for 7 DAG stages.
- **Automated Test Suite:** **77 / 77 tests passing (100% pass rate)**.
