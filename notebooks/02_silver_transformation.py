# Databricks notebook source
# MAGIC %md
# MAGIC # 02 - Silver Transformation Notebook
# MAGIC ## Medallion Architecture: Bronze to Silver Cleansing, Enrichment & Quality Quarantine
# MAGIC 
# MAGIC **Author:** Retail Data Engineering Pipeline  
# MAGIC **Target Role:** Celebal Technologies Data Engineer Evaluation  
# MAGIC **Lakehouse Layer:** Silver (Conformed, deduplicated, cleansed, typed records)
# MAGIC 
# MAGIC ### Objectives:
# MAGIC 1. Ingest Bronze feeds.
# MAGIC 2. Apply atomic validation rules and isolate defective records into quarantine.
# MAGIC 3. Standardize timestamps, calculate line item derived fields, and deduplicate.
# MAGIC 4. Persist clean records into Silver layer.

# COMMAND ----------
import os
import sys
import csv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.databricks_utils import is_running_on_databricks, get_lakehouse_path
from src.utils.logger import get_logger
from src.validation.quality_checker import DataQualityChecker
from src.validation.rules import (
    NotNullOrEmptyRule,
    NumericRangeRule,
    TimestampFormatRule,
    ReferentialIntegrityRule
)
from src.transformation.cleaner import SilverTransformer

logger = get_logger("Notebook_02_Silver")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Execute Data Quality Enforcement & Silver Transformation

# COMMAND ----------
def load_csv_rows(file_path: str) -> list:
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def write_csv_rows(file_path: str, rows: list):
    if not rows:
        return
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

def run_silver_pipeline():
    bronze_dir = get_lakehouse_path("bronze")
    silver_dir = get_lakehouse_path("silver")
    quarantine_dir = os.path.join(PROJECT_ROOT, "data", "quarantine")
    
    os.makedirs(silver_dir, exist_ok=True)
    os.makedirs(quarantine_dir, exist_ok=True)
    
    tx_file = os.path.join(bronze_dir, "transactions", "transactions_bronze.csv")
    cust_file = os.path.join(bronze_dir, "customers", "customers_bronze.csv")
    prod_file = os.path.join(bronze_dir, "products", "products_bronze.csv")
    
    transactions = load_csv_rows(tx_file)
    customers = load_csv_rows(cust_file)
    products = load_csv_rows(prod_file)
    
    valid_customer_ids = {c["customer_id"].strip() for c in customers if c.get("customer_id")}
    valid_product_ids = {p["product_id"].strip() for p in products if p.get("product_id")}
    
    # 1. Quality & Quarantine Isolation
    checker = DataQualityChecker(dataset_name="transactions")
    checker.set_duplicate_tracker("transaction_id")
    checker.add_rule(NotNullOrEmptyRule(field_name="transaction_id", error_code="ERR_NULL_TX_ID"))
    checker.add_rule(NotNullOrEmptyRule(field_name="customer_id", error_code="ERR_MISSING_CUSTOMER_ID"))
    checker.add_rule(NumericRangeRule(field_name="quantity", min_val=1, max_val=1000, error_code="ERR_INVALID_QUANTITY"))
    checker.add_rule(NumericRangeRule(field_name="unit_price", min_val=0.01, max_val=50000.0, error_code="ERR_INVALID_PRICE"))
    checker.add_rule(TimestampFormatRule(field_name="transaction_timestamp", expected_format="%Y-%m-%d %H:%M:%S", error_code="ERR_MALFORMED_TIMESTAMP"))
    checker.add_rule(ReferentialIntegrityRule(field_name="customer_id", reference_set=valid_customer_ids, reference_name="dim_customer", error_code="ERR_REF_CUSTOMER"))
    checker.add_rule(ReferentialIntegrityRule(field_name="product_id", reference_set=valid_product_ids, reference_name="dim_product", error_code="ERR_REF_PRODUCT"))
    
    clean_txns, quarantine_txns, report = checker.validate_dataset(transactions)
    
    # Save quarantine
    quarantine_path = os.path.join(quarantine_dir, "transactions_quarantine.csv")
    write_csv_rows(quarantine_path, quarantine_txns)
    
    # 2. Conformed Silver Transformation
    transformer = SilverTransformer()
    silver_txns = transformer.clean_transactions(clean_txns)
    
    silver_final_path = os.path.join(silver_dir, "transactions", "transactions_silver.csv")
    write_csv_rows(silver_final_path, silver_txns)
    
    logger.info(f"Silver Transformation Complete: {len(silver_txns):,} clean records, {len(quarantine_txns):,} quarantined.")
    
    return {
        "status": "SUCCESS",
        "clean_records": len(silver_txns),
        "quarantined_records": len(quarantine_txns),
        "silver_destination": silver_final_path
    }

if __name__ == "__main__":
    result = run_silver_pipeline()
    print("Silver Transformation Result:", result)
