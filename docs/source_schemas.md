# Source Data Schemas & Quality Specifications

This document outlines the schema specifications, business definitions, data types, constraints, and intentional anomaly distributions for all source systems ingested by the **Retail Data Engineering Pipeline**.

---

## 1. Source System: Transactions (`transactions.csv`)

- **Format:** CSV with headers, UTF-8 encoding
- **Estimated Volume:** 105,000+ records per initial batch
- **Grain:** One record per item purchased in a transaction

| Column Name | Physical Type | Logical Type | Nullable | Description / Business Rules | Example Value |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `transaction_id` | String | Identifier | No | Unique identifier for transaction (`TXN-XXXXXXX`) | `TXN-0012489` |
| `customer_id` | String | Foreign Key | Yes (Defect) | Reference to `customers.customer_id` | `CUST-00842` |
| `product_id` | String | Foreign Key | No | Reference to `products.product_id` | `PRD-0412` |
| `store_id` | String | Foreign Key | No | Reference to `stores.store_id` | `STR-014` |
| `transaction_timestamp` | String | Timestamp | No | Timestamp of transaction (`YYYY-MM-DD HH:MM:SS`) | `2024-08-14 15:32:01` |
| `quantity` | Integer | Metric | No | Units purchased (Expected $> 0$) | `2` |
| `unit_price` | Float / Decimal | Metric | No | Price per unit in USD (Expected $> 0.00$) | `49.99` |
| `discount` | Float / Decimal | Metric | No | Promotional discount fraction ($0.0 \le d \le 1.0$) | `0.10` |
| `payment_method` | String | Dimension | No | Method of payment (Expected: standardized values) | `Credit Card` |

### Controlled Defects Injected into Transactions:
1. **Missing Customer IDs (~0.25%):** Null or blank strings representing anonymous checkouts or sensor transmission loss.
2. **Invalid Customer IDs (~0.20%):** IDs like `CUST-99XXX` that do not exist in the customer master (tests referential integrity).
3. **Negative / Zero Unit Price (~0.15%):** Prices $\le 0.00$ caused by unhandled return items or POS pricing glitches.
4. **Negative / Zero Quantity (~0.15%):** Quantities $\le 0$ representing invalid cart states.
5. **Malformed Timestamps (~0.12%):** Non-standard strings (`2024/02/31 25:99:99`, `INVALID_DATE_TIME`, `NULL`).
6. **Inconsistent Payment Categoricals (~0.30%):** Unstandardized casing or unknown values (`credit_card`, `CC`, `UNKNOWN`, `Crypto`).
7. **Duplicate Transactions (~0.20%):** Duplicated lines with identical `transaction_id` and timestamps.

---

## 2. Source System: Customers (`customers.csv` & PostgreSQL)

- **Format:** CSV & Relational PostgreSQL Table (`retail_source.customers`)
- **Volume:** 10,500 records
- **Grain:** One row per registered customer

| Column Name | Type | Constraint | Description | Example Value |
| :--- | :--- | :--- | :--- | :--- |
| `customer_id` | VARCHAR(16) | PRIMARY KEY | Unique customer identifier | `CUST-00001` |
| `customer_name` | VARCHAR(128) | NOT NULL | Full name of the customer | `James Smith` |
| `email` | VARCHAR(128) | NOT NULL | Customer contact email | `james.smith1@example.com` |
| `city` | VARCHAR(64) | NOT NULL | Customer residential city | `Dallas` |
| `state` | VARCHAR(8) | NOT NULL | State abbreviation (2-letter) | `TX` |
| `signup_date` | DATE | NOT NULL | Customer registration date | `2022-04-19` |
| `customer_segment` | VARCHAR(32) | NOT NULL | Tier: `Standard`, `Premium`, `VIP`, `Corporate` | `Premium` |

---

## 3. Source System: Products (`products.csv`)

- **Format:** CSV with headers, UTF-8 encoding
- **Volume:** 1,200 records
- **Grain:** One row per catalog SKU

| Column Name | Type | Constraint | Description | Example Value |
| :--- | :--- | :--- | :--- | :--- |
| `product_id` | VARCHAR(16) | PRIMARY KEY | Unique SKU identifier | `PRD-0001` |
| `product_name` | VARCHAR(128) | NOT NULL | Descriptive product title | `Sony Audio Model 452` |
| `category` | VARCHAR(64) | NOT NULL | Top-level category | `Electronics` |
| `subcategory` | VARCHAR(64) | NOT NULL | Granular subcategory | `Audio` |
| `brand` | VARCHAR(64) | NOT NULL | Manufacturer or brand | `Sony` |
| `unit_cost` | DECIMAL(10,2) | $> 0.00$ | Baseline acquisition / wholesale cost | `85.50` |
| `recommended_price` | DECIMAL(10,2) | $> unit\_cost$ | Suggested retail price (MSRP) | `119.99` |

---

## 4. Source System: Store Information (`stores.csv`)

- **Format:** CSV with headers, UTF-8 encoding
- **Volume:** 55 records
- **Grain:** One row per physical retail store location

| Column Name | Type | Constraint | Description | Example Value |
| :--- | :--- | :--- | :--- | :--- |
| `store_id` | VARCHAR(16) | PRIMARY KEY | Unique store identifier | `STR-001` |
| `store_name` | VARCHAR(128) | NOT NULL | Location branch name | `RetailOne New York Flagship` |
| `city` | VARCHAR(64) | NOT NULL | Store operating city | `New York` |
| `state` | VARCHAR(8) | NOT NULL | State code | `NY` |
| `region` | VARCHAR(32) | NOT NULL | Territory: `North`, `South`, `East`, `West`, `Central` | `East` |
| `store_type` | VARCHAR(32) | NOT NULL | `Flagship`, `Supercenter`, `Express`, `Mall Outlet` | `Flagship` |
| `opened_date` | DATE | NOT NULL | Store grand opening date | `2018-06-12` |

---

## 5. Source System: External REST API (`Exchange Rates`)

- **Format:** JSON REST API response
- **Endpoint:** `GET /api/v1/rates?base=USD`
- **Purpose:** Macroeconomic conversion rates to normalize foreign currency transactions or multi-currency retail reporting into USD.

```json
{
  "base": "USD",
  "date": "2026-09-26",
  "rates": {
    "EUR": 0.92,
    "GBP": 0.79,
    "INR": 83.45,
    "CAD": 1.36,
    "AUD": 1.52,
    "JPY": 155.20
  }
}
```
