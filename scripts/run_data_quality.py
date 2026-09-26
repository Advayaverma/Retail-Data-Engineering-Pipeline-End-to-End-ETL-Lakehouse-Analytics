#!/usr/bin/env python3
"""
Operational Data Quality & Quarantine Runner (Phase 5).
Applies validation rules to Bronze transactions, extracts referential sets from Customer and Product
masters, isolates anomalies into the Quarantine layer, and outputs executive audit summaries.
"""

import os
import sys
import csv

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger
from src.validation.rules import (
    NotNullOrEmptyRule,
    NumericRangeRule,
    TimestampFormatRule,
    ReferentialIntegrityRule,
)
from src.validation.quality_checker import DataQualityChecker

BRONZE_DIR = os.path.join(PROJECT_ROOT, "data", "bronze")
QUARANTINE_DIR = os.path.join(PROJECT_ROOT, "data", "quarantine")
SILVER_STAGING_DIR = os.path.join(PROJECT_ROOT, "data", "silver", "staging")
DOCS_DIR = os.path.join(PROJECT_ROOT, "docs")
LOGS_DIR = os.path.join(PROJECT_ROOT, "logs")


def load_csv_rows(file_path: str) -> list:
    """Helper to read CSV rows as dictionaries."""
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def run_data_quality_pipeline() -> dict:
    """Execute complete data quality audit across bronze datasets."""
    logger = get_logger("pipeline.data_quality")
    logger.info("=" * 70)
    logger.info("STARTING DATA QUALITY & QUARANTINE EVALUATION (PHASE 5)")
    logger.info("=" * 70)

    # 1. Load Bronze datasets
    txn_file = os.path.join(BRONZE_DIR, "transactions", "transactions_bronze.csv")
    cust_file = os.path.join(BRONZE_DIR, "customers", "customers_bronze.csv")
    prod_file = os.path.join(BRONZE_DIR, "products", "products_bronze.csv")
    store_file = os.path.join(BRONZE_DIR, "stores", "stores_bronze.csv")

    transactions = load_csv_rows(txn_file)
    customers = load_csv_rows(cust_file)
    products = load_csv_rows(prod_file)
    stores = load_csv_rows(store_file)

    logger.info(f"Loaded {len(transactions):,} bronze transactions for validation.")

    # 2. Build Reference Sets for Foreign Key validation
    valid_customer_ids = {c["customer_id"].strip() for c in customers if c.get("customer_id")}
    valid_product_ids = {p["product_id"].strip() for p in products if p.get("product_id")}
    valid_store_ids = {s["store_id"].strip() for s in stores if s.get("store_id")}

    # 3. Configure Data Quality Rules
    checker = DataQualityChecker(dataset_name="transactions")
    checker.set_duplicate_tracker(key_field="transaction_id")
    checker.add_rule(NotNullOrEmptyRule(field_name="transaction_id", error_code="ERR_MISSING_TXN_ID"))
    checker.add_rule(NotNullOrEmptyRule(field_name="customer_id", error_code="ERR_MISSING_CUSTOMER_ID"))
    checker.add_rule(ReferentialIntegrityRule("customer_id", valid_customer_ids, "customers", "ERR_REF_CUSTOMER"))
    checker.add_rule(ReferentialIntegrityRule("product_id", valid_product_ids, "products", "ERR_REF_PRODUCT"))
    checker.add_rule(ReferentialIntegrityRule("store_id", valid_store_ids, "stores", "ERR_REF_STORE"))
    checker.add_rule(NumericRangeRule("unit_price", min_val=0.01, error_code="ERR_INVALID_PRICE"))
    checker.add_rule(NumericRangeRule("quantity", min_val=1, error_code="ERR_INVALID_QUANTITY"))
    checker.add_rule(NumericRangeRule("discount", min_val=0.0, max_val=1.0, error_code="ERR_INVALID_DISCOUNT"))
    checker.add_rule(TimestampFormatRule("transaction_timestamp", "%Y-%m-%d %H:%M:%S", "ERR_MALFORMED_TIMESTAMP"))

    # 4. Evaluate quality rules
    clean_records, quarantine_records, report = checker.validate_dataset(transactions)

    # 5. Persist Quarantined Records
    quarantine_path = checker.save_quarantine(quarantine_records, QUARANTINE_DIR)

    # 6. Persist Clean Records ready for Silver
    os.makedirs(SILVER_STAGING_DIR, exist_ok=True)
    clean_path = os.path.join(SILVER_STAGING_DIR, "transactions_clean.csv")
    if clean_records:
        with open(clean_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(clean_records[0].keys()))
            writer.writeheader()
            writer.writerows(clean_records)
    logger.info(f"Routed {len(clean_records):,} clean records to Silver Staging: {clean_path}")

    # 7. Print and save executive report
    report_text = checker.format_report_text(report)
    print("\n" + report_text + "\n")

    os.makedirs(LOGS_DIR, exist_ok=True)
    with open(os.path.join(LOGS_DIR, "data_quality_report.txt"), "w", encoding="utf-8") as f:
        f.write(report_text)

    # Save Markdown summary in docs/
    os.makedirs(DOCS_DIR, exist_ok=True)
    md_content = f"""# Data Quality Audit Report & Quarantine Summary

- **Evaluation Date:** {report['timestamp']}
- **Dataset Evaluated:** `{report['dataset']}`
- **Source Layer:** Bronze (`data/bronze/transactions/transactions_bronze.csv`)
- **Quarantine Target:** `{quarantine_path}`
- **Clean Stream Target:** `{clean_path}`

---

## Executive Summary

| Metric | Count | Percentage |
| :--- | :--- | :--- |
| **Total Rows Ingested** | **{report['rows_processed']:,}** | 100.00% |
| **Clean / Valid Rows Passed** | **{report['valid_rows']:,}** | **{100.0 - report['rejection_rate_pct']:.2f}%** |
| **Quarantined Defect Rows** | **{report['quarantine_rows']:,}** | **{report['rejection_rate_pct']:.2f}%** |

---

## Detailed Anomaly Breakdown

| Defect Category | Error Code | Violations Detected | Remediation / Quarantine Strategy |
| :--- | :--- | :--- | :--- |
| **Duplicate Transactions** | `ERR_DUPLICATE_RECORD` | {report['defect_breakdown']['duplicate_rows']:,} | Isolated to prevent double-counting in financial facts |
| **Missing Customer IDs** | `ERR_MISSING_CUSTOMER_ID` | {report['defect_breakdown']['missing_customer_ids']:,} | Routed to quarantine for guest/anonymous resolution |
| **Invalid Customer IDs** | `ERR_REF_CUSTOMER` | {report['defect_breakdown']['invalid_customer_ids']:,} | Fails foreign key lookup against Customer Master |
| **Invalid Unit Prices** | `ERR_INVALID_PRICE` | {report['defect_breakdown']['invalid_prices']:,} | Negative or zero price anomaly |
| **Invalid Quantities** | `ERR_INVALID_QUANTITY` | {report['defect_breakdown']['invalid_quantities']:,} | Negative or zero items purchased |
| **Malformed Timestamps** | `ERR_MALFORMED_TIMESTAMP` | {report['defect_breakdown']['invalid_timestamps']:,} | Non-parseable or corrupt date strings |
| **Other Anomalies** | Multiple | {report['defect_breakdown']['other_defects']:,} | Unclassified edge cases |

---

## Key Interview Talking Points (Celebal Technologies)

1. **Why Quarantine instead of silent dropping?**
   * Dropping bad data silently leads to undetectable revenue discrepancies between accounting and data platforms.
   * By routing anomalies to `data/quarantine/` with `_defect_code` and `_quarantined_at`, upstream source teams can inspect root causes and resubmit corrected records.
2. **Medallion Gatekeeper:**
   * The Bronze layer retains raw records unconditionally.
   * The Data Quality layer acts as the gatekeeper, ensuring the Silver layer contains only pristine, trusted data.
"""
    with open(os.path.join(DOCS_DIR, "data_quality_summary.md"), "w", encoding="utf-8") as f:
        f.write(md_content)

    return report


if __name__ == "__main__":
    run_data_quality_pipeline()
