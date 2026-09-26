#!/usr/bin/env python3
"""
Operational Star Schema & SCD Type 2 Assembly Runner (Phase 8).
Assembles the complete analytical Kimball Dimensional Model into the Gold layer.
"""

import os
import sys
import csv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger
from src.modelling.star_schema_builder import StarSchemaBuilder

RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")


def load_csv(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run_build_star_schema() -> dict:
    logger = get_logger("pipeline.star_schema_runner")
    logger.info("=" * 70)
    logger.info("STARTING KIMBALL STAR SCHEMA & SCD TYPE 2 ASSEMBLY (PHASE 8)")
    logger.info("=" * 70)

    builder = StarSchemaBuilder(gold_dir=GOLD_DIR)

    # 1. Build Date Dimension
    dim_date = builder.build_dim_date()

    # 2. Build Store Dimension
    raw_stores = load_csv(os.path.join(RAW_DIR, "stores.csv"))
    dim_store = builder.build_dim_store(raw_stores)

    # 3. Build Product Dimension
    raw_products = load_csv(os.path.join(RAW_DIR, "products.csv"))
    dim_product = builder.build_dim_product(raw_products)

    # 4. Build Customer Dimension with SCD Type 2 updates
    raw_customers = load_csv(os.path.join(RAW_DIR, "customers.csv"))
    customer_updates = load_csv(os.path.join(RAW_DIR, "customers_updates_day2.csv"))
    dim_customer = builder.build_dim_customer_scd2(raw_customers, updates=customer_updates)

    # 5. Build Fact Sales Table
    silver_txns = load_csv(os.path.join(SILVER_DIR, "transactions", "transactions_silver.csv"))
    fact_sales = builder.build_fact_sales(
        silver_txns=silver_txns,
        dim_customers=dim_customer,
        dim_products=dim_product,
        dim_stores=dim_store,
    )

    # Audit SCD Type 2 records
    current_cust_count = sum(1 for c in dim_customer if c["is_current"])
    historical_cust_count = sum(1 for c in dim_customer if not c["is_current"])

    logger.info("=" * 70)
    logger.info("KIMBALL STAR SCHEMA ASSEMBLY COMPLETE:")
    logger.info("=" * 70)
    logger.info(f"  * Fact Table: fact_sales             : {len(fact_sales):>8,} rows")
    logger.info(f"  * Dimension:  dim_date               : {len(dim_date):>8,} rows (2021 to 2026)")
    logger.info(f"  * Dimension:  dim_product            : {len(dim_product):>8,} SKUs")
    logger.info(f"  * Dimension:  dim_store              : {len(dim_store):>8,} branches")
    logger.info(f"  * Dimension:  dim_customer (SCD2)   : {len(dim_customer):>8,} total records")
    logger.info(f"      - Active Customers (is_current=True) : {current_cust_count:>8,}")
    logger.info(f"      - Historical Audit (is_current=False): {historical_cust_count:>8,}")
    logger.info("=" * 70)

    return {
        "fact_sales_count": len(fact_sales),
        "dim_date_count": len(dim_date),
        "dim_store_count": len(dim_store),
        "dim_product_count": len(dim_product),
        "dim_customer_total": len(dim_customer),
        "dim_customer_current": current_cust_count,
        "dim_customer_historical": historical_cust_count,
    }


if __name__ == "__main__":
    run_build_star_schema()
