# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Incremental Delta Processing & MERGE Notebook
# MAGIC ## Day-2 Incremental Upsert via Delta Lake ACID Protocol & Time Travel Audit
# MAGIC 
# MAGIC **Author:** Retail Data Engineering Pipeline  
# MAGIC **Target Role:** Celebal Technologies Data Engineer Evaluation  
# MAGIC **Lakehouse Layer:** Silver/Gold Incremental MERGE (SCD Type 1 Upsert & Time Travel)
# MAGIC 
# MAGIC ### Objectives:
# MAGIC 1. Ingest Day-2 CDC/Delta change feed (new inserts + existing updates).
# MAGIC 2. Perform ACID atomic MERGE (upsert matched, insert not matched).
# MAGIC 3. Inspect commit transaction log (`_delta_log/00000000000000000001.json`).
# MAGIC 4. Execute Time Travel query verifying historical snapshots vs. current state.

# COMMAND ----------
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger
from scripts.run_incremental import run_incremental_pipeline as exec_incremental

logger = get_logger("Notebook_05_IncrementalMerge")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Run Incremental Upsert

# COMMAND ----------
def run_incremental_pipeline():
    logger.info("Executing Incremental Delta Pipeline with ACID MERGE and Time Travel...")
    summary = exec_incremental()
    return {
        "status": "SUCCESS",
        "incremental_summary": summary
    }

if __name__ == "__main__":
    merge_result = run_incremental_pipeline()
    print("Incremental Merge Result:", merge_result)
