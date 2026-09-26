# Delta Lake & Incremental Processing Architecture Deep Dive

**Target Role:** Data Engineer — Celebal Technologies Interview Preparation  
**Engine:** Delta Lake 3.2+ Storage Protocol & PySpark `MERGE INTO`  
**Pipeline Stage:** Incremental Silver Lakehouse Processing

---

## 1. What Problem Does Delta Lake Solve?

In traditional data lakes (plain Parquet or CSV on S3/ADLS/HDFS):
1. **No ACID Guarantees:** If an ETL job fails halfway through writing files, corrupted/partial data is visible to downstream consumers.
2. **No Upsert (`MERGE`) Capability:** To update 500 records in a 100,000-row table, engineers were forced to rewrite the **entire table**, wasting compute and I/O.
3. **Small File Problem & File Clutter:** Append-only ingestion creates millions of tiny files, overwhelming object store metadata.
4. **No Audit or Time Travel:** Impossible to query the exact state of data as of yesterday or rollback a corrupted batch.

**Delta Lake** solves this by storing data in immutable Parquet files paired with a transactional log (`_delta_log/`).

---

## 2. The Delta Lake Transaction Protocol (`_delta_log/`)

Every change to a Delta table creates a sequential JSON commit log:
```text
data/silver/delta_transactions/
│
├── _delta_log/
│   ├── 00000000000000000000.json   # Version 0: Day-1 Baseline Write (104,330 rows)
│   └── 00000000000000000001.json   # Version 1: Day-2 Incremental MERGE (500 updated, 5,000 inserted)
│
├── part-00000-1727368760000.csv    # Day 1 snapshot files
└── part-00001-1727368761000.csv    # Day 2 snapshot files
```

### Anatomy of a Commit Log (`00000000000000000001.json`):
```json
{"commitInfo":{"version":1,"operation":"MERGE","operationParameters":{"mode":"merge","mergeSchema":true},"isolationLevel":"WriteSerializable","engineInfo":"DeltaLakeEngine/3.2.0"}}
{"metaData":{"id":"delta_transactions","schema":["transaction_id","customer_id","...","loyalty_points_earned"]}}
{"add":{"path":"part-00001-1727368761000.csv","size":845012,"dataChange":true,"stats":{"numRecords":109330}}}
```

---

## 3. Incremental Processing: `MERGE INTO` vs. Full Reload

| Dimension | Full Table Reload (Traditional Lake) | Delta Lake `MERGE INTO` (This Project) |
| :--- | :--- | :--- |
| **Data Processed** | 100% of historical records read and rewritten | Only incoming 5,500 delta rows parsed and merged |
| **I/O Overhead** | High (grows linearly $O(N)$ with table history) | Minimal (proportional to incoming batch size $\Delta N$) |
| **Concurrency** | Read-write collisions during rewrite | Optimistic Concurrency Control (OCC) |
| **Auditability** | History overwritten and lost | Previous versions preserved via Time Travel |

---

## 4. Schema Enforcement vs. Schema Evolution

1. **Schema Enforcement (Default):**
   * If a producer sends records with mismatched columns or unexpected fields, Delta Lake blocks the write and raises an exception, protecting downstream analytics from silent schema corruption.
2. **Schema Evolution (`mergeSchema=True`):**
   * When intentional new features are launched (such as `loyalty_points_earned` in Day 2), Delta Lake automatically merges the incoming schema, populating `NULL` for historical rows while retaining backwards compatibility.

---

## 5. Time Travel & Audit Mechanics

Because Delta files are immutable and versioned:
```python
# Querying historical Day-1 baseline
v0_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)

# Querying current post-MERGE state
v1_df = spark.read.format("delta").option("versionAsOf", 1).load(delta_path)
```
In our implementation, querying Version 0 returns exactly 104,330 rows with original quantities, while Version 1 returns 109,330 rows with updated payment methods and loyalty points!
