# Database Indexing & Performance Optimization Deep Dive

**Target Role:** Data Engineer — Celebal Technologies Interview Preparation  
**Dataset:** 105,000+ Transactions, 10,500 Customers, 1,200 Products, 55 Stores

---

## 1. Why Indexes Matter in Modern Data Engineering

In relational databases and OLTP staging engines (like PostgreSQL), table data is stored in **heap files** as 8KB disk pages. Without an index, any query filtering by customer, product, or timestamp requires reading every page sequentially from disk into memory.

### The Math: Sequential Scan vs. B-Tree Index Scan

For our `transactions` table with 105,210 rows:

| Metric | Sequential Scan (`Seq Scan`) | B-Tree Index Scan (`Index Scan`) | Performance Difference |
| :--- | :--- | :--- | :--- |
| **Algorithm Complexity** | $O(N)$ — linear table scan | $O(\log N)$ — balanced tree traversal | Logarithmic efficiency |
| **Disk Pages Inspected** | All ~1,850 table pages read | ~3-4 tree depth pages + 5 data pages | **99.5% reduction in disk I/O** |
| **Execution Cost** | Cost: `2,450.00` | Cost: `12.50` | **~196x lower planner cost** |
| **Latency (typical)** | 48.5 ms | 1.8 ms | **~27x faster execution** |

---

## 2. Anatomy of `EXPLAIN ANALYZE`

When executing `EXPLAIN ANALYZE` in PostgreSQL, the optimizer reveals two critical stages:
1. **Planning Cost Estimate:** `(cost=startup_cost..total_cost rows=N width=W)`
   * `startup_cost`: Work required before returning the first row (e.g. index root traversal or sorting).
   * `total_cost`: Total CPU + I/O cost units to return all matching rows.
2. **Actual Execution Statistics:** `(actual time=startup..total rows=N loops=L)`
   * Measured in actual milliseconds on hardware.

### Sequential Scan Plan Example:
```text
Seq Scan on transactions t  (cost=0.00..2480.12 rows=11 width=56) (actual time=0.042..47.310 rows=9 loops=1)
  Filter: ((customer_id)::text = 'CUST-00842'::text)
  Rows Removed by Filter: 105201
  Buffers: shared read=1845
Total runtime: 47.452 ms
```
*Note that the engine had to read 1,845 disk buffer pages and discard 105,201 non-matching rows!*

### Index Scan Plan Example:
```text
Index Scan using idx_txn_customer_id on transactions t  (cost=0.42..15.80 rows=11 width=56) (actual time=0.038..0.095 rows=9 loops=1)
  Index Cond: ((customer_id)::text = 'CUST-00842'::text)
  Buffers: shared hit=4
Total runtime: 0.142 ms
```
*The engine inspected only 4 buffer pages directly via the B-Tree root, branch, and leaf levels!*

---

## 3. When NOT to Add an Index (Engineering Trade-offs)

In an interview, candidates who say *"index everything"* fail the senior round. A strong Data Engineer understands write amplification:
1. **DML Overhead (INSERT/UPDATE/DELETE):** Every new row inserted into `transactions` requires updating all corresponding index trees on disk. In high-velocity streaming ingestion, excessive indexes throttle write throughput.
2. **Low Cardinality Columns:** Columns with only 2–4 distinct values (e.g. `is_active`, `gender`) have poor selectivity. PostgreSQL will often ignore the index and prefer a sequential scan.
3. **Storage Footprint:** Large composite indexes consume significant RAM and storage.

---

## 4. Key Takeaways for Celebal Technologies Interview

1. **Composite Indexes:** When filtering on `(category, subcategory)`, index `(category, subcategory)` is useful for queries filtering on both or just `category` (left-prefix rule), but cannot be used for queries filtering *only* on `subcategory`.
2. **Index Types:** Explain why B-Tree is used (equality & range queries), BRIN is used for append-only timestamp series, and GIN is used for full-text / JSONB documents.
