# PySpark Architecture & Transformation Engine Deep Dive

**Target Role:** Celebal Technologies Data Engineer Evaluation  
**Engine:** Apache Spark 3.5+ / PySpark DataFrame API  
**Pipeline Stages:** Bronze $\to$ Silver (Cleansing & Standardization) and Silver $\to$ Gold (Star Schema & Business Aggregates)

---

## 1. Why PySpark for Retail Data Engineering?

Traditional single-node tools (Pandas) load entire datasets into driver RAM, failing with `OutOfMemory` (OOM) errors as transactional volumes scale into millions of rows. **Apache Spark** operates on a distributed master-worker cluster architecture:
- **Resilient Distributed Datasets (RDDs):** Fault-tolerant, immutable distributed collections partitioned across executors.
- **DataFrames:** Schema-aware tabular abstractions optimized by the **Catalyst Optimizer** and Tungsten binary execution engine.

---

## 2. Operation-by-Operation Technical Justification

Every Spark operation in this project was deliberately chosen for distributed execution efficiency:

### A. `StructType` Schema Enforcement vs. `inferSchema`
```python
raw_schema = StructType([
    StructField("transaction_id", StringType(), False),
    ...
])
df = spark.read.schema(raw_schema).csv("...")
```
- **Why?** Setting `inferSchema=True` forces Spark to trigger an extra preliminary job scanning the entire dataset to guess data types, doubling I/O latency. Explicit `StructType` enforces contract guarantees at zero runtime scan cost.

### B. `select` vs. `withColumn`
- `select()` prunes columns at the read boundary, enabling Catalyst **columnar projection pushdown** (reading only necessary columns from disk).
- `withColumn()` appends or replaces specific derived fields (`net_amount`, `gross_amount`).

### C. `filter()` Predicate Pushdown
```python
df.filter(F.col("unit_price") > 0.0)
```
- Spark pushes the filter condition directly down into the file reader level (**predicate pushdown**), discarding invalid rows before they enter executor memory.

### D. Broadcast Hash Join (`F.broadcast(df_dimension)`)
```python
df_fact = df_txn.join(F.broadcast(df_products), on="product_id", how="inner")
```
- **The Problem:** A standard Sort-Merge Join (`SortMergeJoin`) hashes and shuffles millions of transaction records across all cluster nodes, incurring massive network I/O.
- **The Solution:** Since `products` (1,200 rows) and `stores` (55 rows) are under the default broadcast threshold (10MB), `F.broadcast()` replicates the small dimension to all worker nodes. Transactions stay in place, achieving an $O(1)$ local memory hash lookup with **zero network shuffle**!

### E. `repartition()` vs. `coalesce()`
| Operation | Shuffle Type | When Used |
| :--- | :--- | :--- |
| `repartition(N, "year", "month")` | **Wide Dependency (Full Shuffle)** | Reshuffling data evenly across cluster executors or preparing directory partition columns. |
| `coalesce(N)` | **Narrow Dependency (No Shuffle)** | Collapsing 200 small output partitions into 4 consolidated files to solve the Hadoop/Spark **Small File Problem**. |

### F. Window Functions (`pyspark.sql.window.Window`)
```python
monthly_window = Window.partitionBy("txn_year", "txn_month").orderBy(F.col("net_revenue").desc())
df.withColumn("category_rank", F.dense_rank().over(monthly_window))
```
- Performs intra-partition ranking and moving totals without collapsing rows, preserving transaction-level grain while evaluating window aggregates.

---

## 3. Catalyst Optimizer & Adaptive Query Execution (AQE)

In [`src/utils/spark_session.py`](file:///c:/Users/Advaya/OneDrive/Desktop/project/src/utils/spark_session.py), we configure:
1. `spark.sql.adaptive.enabled = true`: Dynamically converts Sort-Merge joins into Broadcast joins at runtime if intermediate filtered stages shrink in size.
2. `spark.sql.adaptive.coalescePartitions.enabled = true`: Automatically merges post-shuffle partitions to prevent empty partition overhead.
3. `spark.sql.shuffle.partitions = 4`: Overrides the default 200 shuffle partitions for optimal local executor throughput.
