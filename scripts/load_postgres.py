#!/usr/bin/env python3
"""
Data Loading & Relational Database Setup Script (Phase 4).
Loads CSV datasets into PostgreSQL (or local SQLite relational warehouse for offline testing),
applies relational schemas, constraints, and creates B-Tree indexes.
"""

import os
import sys
import csv
import sqlite3
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
SQLITE_DB_PATH = os.path.join(PROJECT_ROOT, "data", "retail_dw.db")


def load_to_sqlite(db_path: str = SQLITE_DB_PATH) -> dict:
    """
    Creates relational tables, constraints, and loads data into SQLite.
    Provides identical SQL semantics to validate queries offline.
    """
    logger = get_logger("db.loader")
    logger.info(f"Setting up relational database at: {db_path}")

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON;")

    # 1. Stores
    cursor.execute("DROP TABLE IF EXISTS stores;")
    cursor.execute("""
        CREATE TABLE stores (
            store_id     VARCHAR(16) PRIMARY KEY,
            store_name   VARCHAR(128) NOT NULL,
            city         VARCHAR(64)  NOT NULL,
            state        VARCHAR(8)   NOT NULL,
            region       VARCHAR(32)  NOT NULL,
            store_type   VARCHAR(32)  NOT NULL,
            opened_date  DATE         NOT NULL
        );
    """)

    # 2. Products
    cursor.execute("DROP TABLE IF EXISTS products;")
    cursor.execute("""
        CREATE TABLE products (
            product_id        VARCHAR(16) PRIMARY KEY,
            product_name      VARCHAR(128) NOT NULL,
            category          VARCHAR(64)  NOT NULL,
            subcategory       VARCHAR(64)  NOT NULL,
            brand             VARCHAR(64)  NOT NULL,
            unit_cost         NUMERIC(10, 2) NOT NULL,
            recommended_price NUMERIC(10, 2) NOT NULL
        );
    """)

    # 3. Customers
    cursor.execute("DROP TABLE IF EXISTS customers;")
    cursor.execute("""
        CREATE TABLE customers (
            customer_id      VARCHAR(16) PRIMARY KEY,
            customer_name    VARCHAR(128) NOT NULL,
            email            VARCHAR(128) NOT NULL,
            city             VARCHAR(64)  NOT NULL,
            state            VARCHAR(8)   NOT NULL,
            signup_date      DATE         NOT NULL,
            customer_segment VARCHAR(32)  NOT NULL
        );
    """)

    # 4. Transactions
    cursor.execute("DROP TABLE IF EXISTS transactions;")
    cursor.execute("""
        CREATE TABLE transactions (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id        VARCHAR(32)    NOT NULL,
            customer_id           VARCHAR(16),
            product_id            VARCHAR(16)    NOT NULL,
            store_id              VARCHAR(16)    NOT NULL,
            transaction_timestamp TEXT           NOT NULL,
            quantity              INT            NOT NULL,
            unit_price            NUMERIC(10, 2) NOT NULL,
            discount              NUMERIC(4, 2)  DEFAULT 0.00,
            payment_method        VARCHAR(32)    NOT NULL
        );
    """)

    # Create Indexes
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_txn_customer ON transactions (customer_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_txn_product ON transactions (product_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_txn_store ON transactions (store_id);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_txn_timestamp ON transactions (transaction_timestamp);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_cust_segment ON customers (customer_segment, state);")

    # Load Stores
    with open(os.path.join(RAW_DIR, "stores.csv"), "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        store_rows = [(r["store_id"], r["store_name"], r["city"], r["state"], r["region"], r["store_type"], r["opened_date"]) for r in reader]
    cursor.executemany("INSERT INTO stores VALUES (?, ?, ?, ?, ?, ?, ?)", store_rows)

    # Load Products
    with open(os.path.join(RAW_DIR, "products.csv"), "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        prod_rows = [(r["product_id"], r["product_name"], r["category"], r["subcategory"], r["brand"], float(r["unit_cost"]), float(r["recommended_price"])) for r in reader]
    cursor.executemany("INSERT INTO products VALUES (?, ?, ?, ?, ?, ?, ?)", prod_rows)

    # Load Customers
    with open(os.path.join(RAW_DIR, "customers.csv"), "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        cust_rows = [(r["customer_id"], r["customer_name"], r["email"], r["city"], r["state"], r["signup_date"], r["customer_segment"]) for r in reader]
    cursor.executemany("INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?, ?)", cust_rows)

    # Load Transactions
    with open(os.path.join(RAW_DIR, "transactions.csv"), "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        txn_rows = []
        for r in reader:
            txn_rows.append((
                r["transaction_id"],
                r["customer_id"] if r["customer_id"].strip() else None,
                r["product_id"],
                r["store_id"],
                r["transaction_timestamp"],
                int(r["quantity"]),
                float(r["unit_price"]),
                float(r["discount"]),
                r["payment_method"]
            ))
    cursor.executemany("INSERT INTO transactions (transaction_id, customer_id, product_id, store_id, transaction_timestamp, quantity, unit_price, discount, payment_method) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", txn_rows)

    conn.commit()
    conn.close()

    counts = {
        "stores": len(store_rows),
        "products": len(prod_rows),
        "customers": len(cust_rows),
        "transactions": len(txn_rows),
    }
    logger.info(f"Relational database successfully populated: {counts}")
    return counts


def main():
    print("=" * 70)
    print("Retail Data Engineering Pipeline — Relational Database Setup (Phase 4)")
    print("=" * 70)
    counts = load_to_sqlite()
    print("\nDatabase Loading Summary:")
    for tbl, cnt in counts.items():
        print(f"  * {tbl.title():<15}: {cnt:>8,} rows inserted")
    print(f"\nDatabase file generated at: {SQLITE_DB_PATH}")
    print("=" * 70)


if __name__ == "__main__":
    main()
