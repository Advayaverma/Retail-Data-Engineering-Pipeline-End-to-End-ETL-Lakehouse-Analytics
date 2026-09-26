#!/usr/bin/env python3
"""
Operational Incremental Processing & Delta Lake Simulator (Phase 7).

Demonstrates:
1. Day 1 Baseline write (104,330 records) into Delta Table -> Version 0
2. Day 2 Delta Batch generation (5,000 new transactions + 500 updated transactions)
3. Schema Evolution: Introducing 'loyalty_points_earned' column
4. Delta MERGE execution: 500 updated, 5,000 inserted -> Version 1
5. Time Travel validation: Querying Version 0 vs Version 1

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
import sys
import csv
import random

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger
from src.transformation.delta_lake_engine import DeltaTable

SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")
DELTA_DIR = os.path.join(SILVER_DIR, "delta_transactions")
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")


def load_csv(path: str) -> list:
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def run_incremental_pipeline() -> dict:
    logger = get_logger("pipeline.incremental")
    logger.info("=" * 70)
    logger.info("STARTING DELTA LAKE INCREMENTAL PROCESSING PIPELINE (PHASE 7)")
    logger.info("=" * 70)

    delta_table = DeltaTable(table_path=DELTA_DIR)

    # -------------------------------------------------------------------------
    # STEP 1: DAY 1 BASELINE LOAD (Version 0)
    # -------------------------------------------------------------------------
    logger.info("--- [Step 1] Loading Day 1 Baseline into Delta Table ---")
    day1_records = load_csv(os.path.join(SILVER_DIR, "transactions", "transactions_silver.csv"))
    v0 = delta_table.write(day1_records, mode="overwrite", merge_schema=False)
    logger.info(f"Day 1 Baseline committed to Delta Table as Version {v0} ({len(day1_records):,} records).")

    # -------------------------------------------------------------------------
    # STEP 2: PREPARE DAY 2 DELTA BATCH (New + Modified Records)
    # -------------------------------------------------------------------------
    logger.info("--- [Step 2] Preparing Day 2 Delta Batch ---")
    day2_raw = load_csv(os.path.join(RAW_DIR, "transactions_day2.csv"))
    
    # Take 5,000 new transactions
    new_txns = []
    for r in day2_raw[:5000]:
        item = dict(r)
        qty = int(item["quantity"])
        price = float(item["unit_price"])
        disc = float(item["discount"])
        gross = round(qty * price, 2)
        net = round(gross * (1 - disc), 2)
        item.update({
            "txn_date": "2026-01-01",
            "txn_year": "2026",
            "txn_month": "1",
            "gross_amount": str(gross),
            "discount_amount": str(round(gross * disc, 2)),
            "net_amount": str(net),
            "loyalty_points_earned": str(int(net * 0.1)),  # New column for Schema Evolution!
        })
        new_txns.append(item)

    # Take 500 existing records from Day 1 and modify them (e.g. customer returned part of order or price adjusted)
    random.seed(42)
    sample_day1_for_update = random.sample(day1_records, 500)
    updated_txns = []
    for r in sample_day1_for_update:
        upd = dict(r)
        # Modify quantity and recalculate net amount
        upd["quantity"] = "1"
        price = float(upd["unit_price"])
        disc = float(upd["discount"])
        gross = round(1 * price, 2)
        net = round(gross * (1 - disc), 2)
        upd["gross_amount"] = str(gross)
        upd["discount_amount"] = str(round(gross * disc, 2))
        upd["net_amount"] = str(net)
        upd["payment_method"] = "UPI"  # Updated payment method
        upd["loyalty_points_earned"] = str(int(net * 0.1))  # New column
        updated_txns.append(upd)

    combined_day2_batch = new_txns + updated_txns
    logger.info(
        f"Day 2 batch prepared: {len(new_txns):,} new transactions + {len(updated_txns):,} updated transactions "
        f"= {len(combined_day2_batch):,} total records in delta batch."
    )

    # -------------------------------------------------------------------------
    # STEP 3: EXECUTE DELTA MERGE (UPSERT) WITH SCHEMA EVOLUTION
    # -------------------------------------------------------------------------
    logger.info("--- [Step 3] Executing Delta MERGE (Upsert) with Schema Evolution ---")
    updated_cnt, inserted_cnt, v1 = delta_table.merge(
        updates=combined_day2_batch,
        key_field="transaction_id",
        merge_schema=True  # Enables schema evolution for loyalty_points_earned!
    )

    # -------------------------------------------------------------------------
    # STEP 4: TIME TRAVEL DEMONSTRATION
    # -------------------------------------------------------------------------
    logger.info("--- [Step 4] Demonstrating Delta Lake Time Travel ---")
    v0_snapshot = delta_table.read_version(0)
    v1_snapshot = delta_table.read_version(1)

    logger.info(f"Time Travel -> Version 0 Snapshot: {len(v0_snapshot):,} records (Original baseline)")
    logger.info(f"Time Travel -> Version 1 Snapshot: {len(v1_snapshot):,} records (Post-MERGE state)")
    logger.info(f"Columns in Version 0: {len(v0_snapshot[0].keys())} columns")
    logger.info(f"Columns in Version 1: {len(v1_snapshot[0].keys())} columns (includes 'loyalty_points_earned')")

    # Verify a modified record between Version 0 and Version 1
    sample_id = updated_txns[0]["transaction_id"]
    v0_record = next(r for r in v0_snapshot if r["transaction_id"] == sample_id)
    v1_record = next(r for r in v1_snapshot if r["transaction_id"] == sample_id)

    logger.info("-" * 70)
    logger.info(f"RECORD AUDIT FOR '{sample_id}' ACROSS TIME TRAVEL VERSIONS:")
    logger.info(f"  * Version 0: Qty={v0_record['quantity']}, Net=${v0_record['net_amount']}, Payment={v0_record['payment_method']}")
    logger.info(f"  * Version 1: Qty={v1_record['quantity']}, Net=${v1_record['net_amount']}, Payment={v1_record['payment_method']}, Loyalty={v1_record.get('loyalty_points_earned')}")
    logger.info("-" * 70)

    # -------------------------------------------------------------------------
    # STEP 5: COMMIT AUDIT HISTORY
    # -------------------------------------------------------------------------
    history = delta_table.get_history()
    print("\n" + "=" * 70)
    print("DELTA LAKE TRANSACTION LOG COMMIT HISTORY (_delta_log):")
    print("=" * 70)
    for h in history:
        print(f"Version {h.get('version')}: Operation={h.get('operation')}, Engine={h.get('engineInfo')}, Mode={h.get('operationParameters', {}).get('mode')}")
    print("=" * 70)

    return {
        "version_0_count": len(v0_snapshot),
        "version_1_count": len(v1_snapshot),
        "updated_count": updated_cnt,
        "inserted_count": inserted_cnt,
        "history": history,
    }


if __name__ == "__main__":
    run_incremental_pipeline()
