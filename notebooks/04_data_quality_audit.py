# Databricks notebook source
# MAGIC %md
# MAGIC # 04 - Data Quality & Audit Report Notebook
# MAGIC ## Automated Data Quality Rules, Quarantine Analysis & Sla Compliance
# MAGIC 
# MAGIC **Author:** Retail Data Engineering Pipeline  
# MAGIC **Target Role:** Celebal Technologies Data Engineer Evaluation  
# MAGIC **Function:** Operational Quality Dashboard & Audit Trail
# MAGIC 
# MAGIC ### Objectives:
# MAGIC 1. Read quarantined records from `data/quarantine/`.
# MAGIC 2. Categorize failure distributions by defect code (`ERR_NULL_TX_ID`, `ERR_QTY_OUT_OF_RANGE`, etc.).
# MAGIC 3. Calculate pass rates, defect rates, and completeness ratios.
# MAGIC 4. Produce audit summary metrics.

# COMMAND ----------
import os
import sys
from collections import Counter

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

logger = get_logger("Notebook_04_QualityAudit")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Quarantine & Rule Violation Audit

# COMMAND ----------
def audit_data_quality():
    quarantine_path = os.path.join(PROJECT_ROOT, "data", "quarantine", "transactions_quarantine.csv")
    silver_tx_path = os.path.join(PROJECT_ROOT, "data", "silver", "transactions", "transactions_silver.csv")
    
    if not os.path.exists(quarantine_path):
        logger.warning(f"Quarantine file {quarantine_path} not found.")
        return {"status": "NO_QUARANTINE_DATA"}
        
    defect_counter = Counter()
    total_quarantined = 0
    
    with open(quarantine_path, "r", encoding="utf-8") as f:
        header = f.readline().strip().split(",")
        defect_idx = header.index("_defect_code") if "_defect_code" in header else -1
        
        for line in f:
            if line.strip():
                parts = line.strip().split(",")
                code = parts[defect_idx] if defect_idx != -1 and len(parts) > defect_idx else "UNKNOWN"
                defect_counter[code] += 1
                total_quarantined += 1
                
    clean_count = 0
    if os.path.exists(silver_tx_path):
        with open(silver_tx_path, "r", encoding="utf-8") as f:
            clean_count = sum(1 for line in f if line.strip()) - 1
            
    total_records = clean_count + total_quarantined
    pass_rate = (clean_count / total_records * 100) if total_records > 0 else 0.0
    defect_rate = (total_quarantined / total_records * 100) if total_records > 0 else 0.0
    
    report = {
        "total_records_evaluated": total_records,
        "clean_records_passed": clean_count,
        "quarantined_records": total_quarantined,
        "pass_rate_pct": round(pass_rate, 2),
        "defect_rate_pct": round(defect_rate, 2),
        "defects_by_code": dict(defect_counter.most_common())
    }
    
    logger.info("=== Data Quality Audit Results ===")
    logger.info(f"Total Evaluated: {total_records:,}")
    logger.info(f"Clean (Passed): {clean_count:,} ({report['pass_rate_pct']}%)")
    logger.info(f"Quarantined: {total_quarantined:,} ({report['defect_rate_pct']}%)")
    for code, count in report["defects_by_code"].items():
        logger.info(f"  - {code}: {count:,}")
        
    return report

if __name__ == "__main__":
    audit_results = audit_data_quality()
    print("Audit Results:", audit_results)
