#!/usr/bin/env python3
"""
Master Transformation Runner (Phase 6).
Executes Silver cleansing and Gold dimensional / business aggregate transformations,
populating data/silver/ and data/gold/ with audited lakehouse tables.
"""

import os
import sys
import csv
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger
from src.transformation.cleaner import SilverTransformer, GoldTransformer

BRONZE_DIR = os.path.join(PROJECT_ROOT, "data", "bronze")
SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")


def load_csv(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: str, rows: list):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not rows:
        return
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def run_all_transformations() -> dict:
    logger = get_logger("pipeline.transformations")
    logger.info("=" * 70)
    logger.info("STARTING DATA TRANSFORMATION PIPELINE (SILVER & GOLD)")
    logger.info("=" * 70)

    # 1. Load Bronze datasets
    txn_bronze = load_csv(os.path.join(BRONZE_DIR, "transactions", "transactions_bronze.csv"))
    prod_bronze = load_csv(os.path.join(BRONZE_DIR, "products", "products_bronze.csv"))
    store_bronze = load_csv(os.path.join(BRONZE_DIR, "stores", "stores_bronze.csv"))
    cust_bronze = load_csv(os.path.join(BRONZE_DIR, "customers", "customers_bronze.csv"))

    # 2. Bronze -> Silver Cleansing
    silver_trans = SilverTransformer()
    silver_txns = silver_trans.clean_transactions(txn_bronze)

    # Write Silver tables
    silver_txn_path = os.path.join(SILVER_DIR, "transactions", "transactions_silver.csv")
    silver_prod_path = os.path.join(SILVER_DIR, "products", "products_silver.csv")
    silver_store_path = os.path.join(SILVER_DIR, "stores", "stores_silver.csv")
    silver_cust_path = os.path.join(SILVER_DIR, "customers", "customers_silver.csv")

    write_csv(silver_txn_path, silver_txns)
    write_csv(silver_prod_path, prod_bronze)
    write_csv(silver_store_path, store_bronze)
    write_csv(silver_cust_path, cust_bronze)
    logger.info(f"Saved {len(silver_txns):,} cleaned transactions to Silver.")

    # 3. Silver -> Gold Star Schema Fact Assembly
    gold_trans = GoldTransformer()
    fact_sales = gold_trans.build_fact_sales(silver_txns, prod_bronze, store_bronze, cust_bronze)

    gold_fact_path = os.path.join(GOLD_DIR, "fact_sales", "fact_sales_gold.csv")
    write_csv(gold_fact_path, fact_sales)
    logger.info(f"Saved {len(fact_sales):,} Gold Fact Sales records to {gold_fact_path}")

    # 4. Gold Aggregates: Monthly Category Summary
    cat_summary = {}
    for f in fact_sales:
        key = (f["txn_year"], f["txn_month"], f["category"])
        if key not in cat_summary:
            cat_summary[key] = {
                "txn_year": f["txn_year"],
                "txn_month": f["txn_month"],
                "category": f["category"],
                "order_count": 0,
                "total_units_sold": 0,
                "gross_revenue": 0.0,
                "net_revenue": 0.0,
                "total_profit": 0.0,
            }
        cat_summary[key]["order_count"] += 1
        cat_summary[key]["total_units_sold"] += f["quantity"]
        cat_summary[key]["gross_revenue"] = round(cat_summary[key]["gross_revenue"] + f["gross_amount"], 2)
        cat_summary[key]["net_revenue"] = round(cat_summary[key]["net_revenue"] + f["net_amount"], 2)
        cat_summary[key]["total_profit"] = round(cat_summary[key]["total_profit"] + f["profit"], 2)

    cat_rows = list(cat_summary.values())
    write_csv(os.path.join(GOLD_DIR, "monthly_category_summary.csv"), cat_rows)
    logger.info(f"Saved {len(cat_rows):,} monthly category summaries to Gold.")

    # 5. Gold Aggregates: Store Performance Summary
    store_summary = {}
    for f in fact_sales:
        sid = f["store_id"]
        if sid not in store_summary:
            store_summary[sid] = {
                "store_id": sid,
                "store_name": f["store_name"],
                "region": f["region"],
                "city": f["store_city"],
                "total_orders": 0,
                "total_units": 0,
                "total_net_revenue": 0.0,
                "total_profit": 0.0,
            }
        store_summary[sid]["total_orders"] += 1
        store_summary[sid]["total_units"] += f["quantity"]
        store_summary[sid]["total_net_revenue"] = round(store_summary[sid]["total_net_revenue"] + f["net_amount"], 2)
        store_summary[sid]["total_profit"] = round(store_summary[sid]["total_profit"] + f["profit"], 2)

    store_rows = list(store_summary.values())
    write_csv(os.path.join(GOLD_DIR, "store_performance_summary.csv"), store_rows)
    logger.info(f"Saved {len(store_rows):,} store performance summaries to Gold.")

    summary = {
        "silver_transactions": len(silver_txns),
        "gold_fact_sales": len(fact_sales),
        "monthly_categories": len(cat_rows),
        "store_summaries": len(store_rows),
    }

    logger.info("=" * 70)
    logger.info("TRANSFORMATION PIPELINE COMPLETE:")
    for k, v in summary.items():
        logger.info(f"  * {k.replace('_', ' ').title():<25}: {v:>8,} rows")
    logger.info("=" * 70)
    return summary


if __name__ == "__main__":
    run_all_transformations()
