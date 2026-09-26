#!/usr/bin/env python3
"""
Gold Star Schema Relational Loader.
Loads dim_date, dim_store, dim_product, dim_customer (SCD2), and fact_sales
into the relational warehouse engine to enable direct SQL analytics.
"""

import os
import sys
import csv
import sqlite3

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")
DB_PATH = os.path.join(PROJECT_ROOT, "data", "retail_dw.db")


def load_gold_star_schema(db_path: str = DB_PATH) -> dict:
    logger = get_logger("db.gold_loader")
    logger.info(f"Loading Gold Star Schema tables into relational warehouse: {db_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. dim_date
    cursor.execute("DROP TABLE IF EXISTS dim_date;")
    cursor.execute("""
        CREATE TABLE dim_date (
            date_sk         INT PRIMARY KEY,
            full_date       TEXT NOT NULL,
            day_of_week     INT NOT NULL,
            day_name        TEXT NOT NULL,
            day_of_month    INT NOT NULL,
            day_of_year     INT NOT NULL,
            week_of_year    INT NOT NULL,
            month_num       INT NOT NULL,
            month_name      TEXT NOT NULL,
            quarter_num     INT NOT NULL,
            quarter_name    TEXT NOT NULL,
            year_num        INT NOT NULL,
            is_weekend      BOOLEAN NOT NULL,
            fiscal_quarter  TEXT NOT NULL,
            fiscal_year     INT NOT NULL
        );
    """)
    date_path = os.path.join(GOLD_DIR, "dim_date", "dim_date.csv")
    with open(date_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        date_rows = [
            (int(r["date_sk"]), r["full_date"], int(r["day_of_week"]), r["day_name"],
             int(r["day_of_month"]), int(r["day_of_year"]), int(r["week_of_year"]),
             int(r["month_num"]), r["month_name"], int(r["quarter_num"]), r["quarter_name"],
             int(r["year_num"]), 1 if r["is_weekend"].lower() == "true" else 0,
             r["fiscal_quarter"], int(r["fiscal_year"]))
            for r in reader
        ]
    cursor.executemany("INSERT INTO dim_date VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", date_rows)

    # 2. dim_store
    cursor.execute("DROP TABLE IF EXISTS dim_store;")
    cursor.execute("""
        CREATE TABLE dim_store (
            store_sk    INT PRIMARY KEY,
            store_id    TEXT NOT NULL,
            store_name  TEXT NOT NULL,
            city        TEXT NOT NULL,
            state       TEXT NOT NULL,
            region      TEXT NOT NULL,
            store_type  TEXT NOT NULL,
            opened_date TEXT NOT NULL
        );
    """)
    store_path = os.path.join(GOLD_DIR, "dim_store", "dim_store.csv")
    with open(store_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        store_rows = [(int(r["store_sk"]), r["store_id"], r["store_name"], r["city"], r["state"], r["region"], r["store_type"], r["opened_date"]) for r in reader]
    cursor.executemany("INSERT INTO dim_store VALUES (?, ?, ?, ?, ?, ?, ?, ?)", store_rows)

    # 3. dim_product
    cursor.execute("DROP TABLE IF EXISTS dim_product;")
    cursor.execute("""
        CREATE TABLE dim_product (
            product_sk        INT PRIMARY KEY,
            product_id        TEXT NOT NULL,
            product_name      TEXT NOT NULL,
            category          TEXT NOT NULL,
            subcategory       TEXT NOT NULL,
            brand             TEXT NOT NULL,
            unit_cost         NUMERIC(10, 2) NOT NULL,
            recommended_price NUMERIC(10, 2) NOT NULL
        );
    """)
    prod_path = os.path.join(GOLD_DIR, "dim_product", "dim_product.csv")
    with open(prod_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        prod_rows = [(int(r["product_sk"]), r["product_id"], r["product_name"], r["category"], r["subcategory"], r["brand"], float(r["unit_cost"]), float(r["recommended_price"])) for r in reader]
    cursor.executemany("INSERT INTO dim_product VALUES (?, ?, ?, ?, ?, ?, ?, ?)", prod_rows)

    # 4. dim_customer (SCD Type 2)
    cursor.execute("DROP TABLE IF EXISTS dim_customer;")
    cursor.execute("""
        CREATE TABLE dim_customer (
            customer_sk          INT PRIMARY KEY,
            customer_id          TEXT NOT NULL,
            customer_name        TEXT NOT NULL,
            email                TEXT NOT NULL,
            city                 TEXT NOT NULL,
            state                TEXT NOT NULL,
            customer_segment     TEXT NOT NULL,
            effective_start_date TEXT NOT NULL,
            effective_end_date   TEXT NOT NULL,
            is_current           BOOLEAN NOT NULL
        );
    """)
    cust_path = os.path.join(GOLD_DIR, "dim_customer", "dim_customer.csv")
    with open(cust_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cust_rows = [
            (int(r["customer_sk"]), r["customer_id"], r["customer_name"], r["email"],
             r["city"], r["state"], r["customer_segment"], r["effective_start_date"],
             r["effective_end_date"], 1 if r["is_current"].lower() == "true" else 0)
            for r in reader
        ]
    cursor.executemany("INSERT INTO dim_customer VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", cust_rows)

    # 5. fact_sales
    cursor.execute("DROP TABLE IF EXISTS fact_sales;")
    cursor.execute("""
        CREATE TABLE fact_sales (
            sales_sk          BIGINT PRIMARY KEY,
            transaction_id    TEXT NOT NULL,
            date_sk           INT NOT NULL,
            customer_sk       INT NOT NULL,
            product_sk        INT NOT NULL,
            store_sk          INT NOT NULL,
            quantity          INT NOT NULL,
            unit_price        NUMERIC(10, 2) NOT NULL,
            gross_sales       NUMERIC(12, 2) NOT NULL,
            discount_rate     NUMERIC(4, 2) NOT NULL,
            discount_amount   NUMERIC(12, 2) NOT NULL,
            net_sales         NUMERIC(12, 2) NOT NULL,
            unit_cost         NUMERIC(10, 2) NOT NULL,
            cost_amount       NUMERIC(12, 2) NOT NULL,
            profit            NUMERIC(12, 2) NOT NULL,
            profit_margin_pct NUMERIC(6, 2) NOT NULL,
            payment_method    TEXT NOT NULL
        );
    """)
    fact_path = os.path.join(GOLD_DIR, "fact_sales", "fact_sales.csv")
    with open(fact_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fact_rows = [
            (int(r["sales_sk"]), r["transaction_id"], int(r["date_sk"]), int(r["customer_sk"]),
             int(r["product_sk"]), int(r["store_sk"]), int(r["quantity"]), float(r["unit_price"]),
             float(r["gross_sales"]), float(r["discount_rate"]), float(r["discount_amount"]),
             float(r["net_sales"]), float(r["unit_cost"]), float(r["cost_amount"]),
             float(r["profit"]), float(r["profit_margin_pct"]), r["payment_method"])
            for r in reader
        ]
    cursor.executemany(
        "INSERT INTO fact_sales VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        fact_rows
    )

    # Build Star Schema Indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_fact_date ON fact_sales (date_sk);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_fact_cust ON fact_sales (customer_sk);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_fact_prod ON fact_sales (product_sk);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_fact_store ON fact_sales (store_sk);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_gold_dim_cust_nat ON dim_customer (customer_id);")

    conn.commit()
    conn.close()

    counts = {
        "dim_date": len(date_rows),
        "dim_store": len(store_rows),
        "dim_product": len(prod_rows),
        "dim_customer": len(cust_rows),
        "fact_sales": len(fact_rows),
    }
    logger.info(f"Gold Star Schema relational tables successfully populated: {counts}")
    return counts


if __name__ == "__main__":
    load_gold_star_schema()
