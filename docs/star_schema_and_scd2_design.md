# Kimball Dimensional Modeling & SCD Type 2 Architecture

**Target Role:** Data Engineer — Celebal Technologies Interview Preparation  
**Architecture:** Kimball Star Schema (Gold Layer)  
**Entities:** `fact_sales`, `dim_date`, `dim_customer` (SCD2), `dim_product` (SCD1), `dim_store`

---

## 1. Why Kimball Star Schema Over 3NF (Third Normal Form)?

In OLTP relational systems, databases are normalized into 3NF to prevent write anomalies during frequent transactions (`INSERT`, `UPDATE`, `DELETE`). However, 3NF creates a web of interconnected tables requiring 10+ joins for every analytical query.

In modern Lakehouse and Data Warehouse architectures (Gold Layer), we model data using **Kimball Dimensional Star Schemas**:

```text
                               +-------------------------+
                               |        dim_date         |
                               +-------------------------+
                               | PK  date_sk             |
                               |     full_date           |
                               |     fiscal_quarter      |
                               +------------+------------+
                                            |
                                            | 1:N
                                            v
+-------------------------+    +-------------------------+    +-------------------------+
|      dim_customer       |    |       fact_sales        |    |       dim_product       |
|      (SCD Type 2)       |    +-------------------------+    |      (SCD Type 1)       |
+-------------------------+    | PK  sales_sk            |    +-------------------------+
| PK  customer_sk         |--->| FK  date_sk             |<---| PK  product_sk          |
| NK  customer_id         |1:N | FK  customer_sk         |1:N | NK  product_id          |
|     customer_segment    |    | FK  product_sk          |    |     category            |
|     effective_start     |    | FK  store_sk            |    |     subcategory         |
|     effective_end       |    |     quantity            |    |     unit_cost           |
|     is_current          |    |     unit_price          |    +-------------------------+
+-------------------------+    |     gross_sales         |
                               |     net_sales           |
                               |     profit              |
                               +------------+------------+
                                            ^
                                            | 1:N
                                            |
                               +------------+------------+
                               |        dim_store        |
                               +-------------------------+
                               | PK  store_sk            |
                               | NK  store_id            |
                               |     region              |
                               +-------------------------+
```

### Business & Technical Advantages:
1. **Simpler SQL Queries:** Business analysts write straightforward queries joining one central fact table with radial dimensions.
2. **Columnar Compression:** Columnar storage engines (Delta Lake / Parquet) achieve high compression ratios on repetitive dimensional attributes.
3. **Optimized for OLAP Scans:** Vectorized execution engines and star-join optimizers easily prune non-matching dimension branches.

---

## 2. Grain of the Fact Table (`fact_sales`)

- **Grain Definition:** One row per individual item purchased within a customer transaction.
- **Degenerate Dimension:** `transaction_id` is preserved in the fact table without a parent dimension table to group line items belonging to the same checkout receipt.
- **Measures Stored:**
  - Fully Additive: `quantity`, `gross_sales`, `discount_amount`, `net_sales`, `cost_amount`, `profit`.
  - Non-Additive / Semi-Additive: `unit_price`, `discount_rate`, `profit_margin_pct`.

---

## 3. Surrogate Keys vs. Natural Keys

- **Natural Key (NK):** The business identifier originating from source systems (e.g., `CUST-00225`, `PRD-0104`). Natural keys can change, be reused, or contain strings that slow down index comparisons.
- **Surrogate Key (SK):** A synthetic, system-generated integer (e.g., `date_sk = 20250615`, `customer_sk = 10542`) with zero business meaning.
  - Surrogate keys decouple the warehouse from source operational changes.
  - **Surrogate keys are mandatory for SCD Type 2**, allowing multiple historical rows for the same natural customer ID.

---

## 4. Slowly Changing Dimension (SCD) Types

| Technique | Behavior | When Used |
| :--- | :--- | :--- |
| **SCD Type 1** | **Overwrite:** Replaces old value with new value. No historical audit trail. | Correcting spelling errors or typos where history does not matter (used for `dim_product`). |
| **SCD Type 2** | **Add New Row:** Expires existing record with `effective_end_date` and `is_current=False`, then inserts new version with `is_current=True`. | Tracking demographic, segment, or territory changes over time (used for `dim_customer`). |
| **SCD Type 3** | **Add New Column:** Keeps current and previous values in two columns (`current_city`, `previous_city`). | Rarely used; limited to only 1 past transition. |

---

## 5. Point-in-Time Fact Joins

When customer `CUST-00225` was living in **Miami as a VIP** in 2024, their purchases in 2024 must reflect VIP status in the South region. When they moved to **New York as Corporate** in 2026, 2026 purchases reflect Corporate in the East region.

```sql
SELECT 
    f.sales_sk,
    c.customer_name,
    c.customer_segment,
    c.city,
    f.net_sales
FROM retail_gold.fact_sales f
INNER JOIN retail_gold.dim_customer c 
    ON f.customer_sk = c.customer_sk;
```
Because `fact_sales` links directly to the point-in-time `customer_sk`, historical reports produce 100% accurate financial attribution without complex time-range joins at query time!
