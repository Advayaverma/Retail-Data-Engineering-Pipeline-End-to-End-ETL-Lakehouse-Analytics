# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Ingestion Notebook
# MAGIC ## Medallion Architecture: Raw Ingestion to Bronze Delta / Raw Storage
# MAGIC 
# MAGIC **Author:** Retail Data Engineering Pipeline  
# MAGIC **Target Role:** Celebal Technologies Data Engineer Evaluation  
# MAGIC **Lakehouse Layer:** Bronze (Append-only Raw Storage with Lineage Metadata)
# MAGIC 
# MAGIC ### Objectives:
# MAGIC 1. Read raw feeds from landed storage (`data/raw/` or Databricks Volumes).
# MAGIC 2. Inject audit tracking metadata (`_ingested_at`, `_source_file`, `_batch_id`).
# MAGIC 3. Persist raw records as Bronze storage for downstream processing.

# COMMAND ----------
import os
import sys
import uuid
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.databricks_utils import is_running_on_databricks, get_lakehouse_path, get_dbutils
from src.utils.logger import get_logger

logger = get_logger("Notebook_01_Bronze")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Initialize Parameters / Widgets

# COMMAND ----------
dbutils = get_dbutils()
batch_id = str(uuid.uuid4())[:8]
ingest_time = datetime.now(timezone.utc).isoformat()

logger.info(f"Initiating Bronze Ingestion | Batch ID: {batch_id} | Ingest Time: {ingest_time}")
print(f"[*] Running on Databricks: {is_running_on_databricks()}")
print(f"[*] Batch ID: {batch_id}")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Ingestion Logic

# COMMAND ----------
def run_bronze_ingestion():
    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    bronze_dir = get_lakehouse_path("bronze")
    os.makedirs(bronze_dir, exist_ok=True)
    
    files_to_ingest = [
        "transactions.csv",
        "customers.csv",
        "products.csv",
        "stores.csv"
    ]
    
    ingested_counts = {}
    
    for filename in files_to_ingest:
        src_path = os.path.join(raw_dir, filename)
        entity_name = filename.replace(".csv", "")
        dest_entity_dir = os.path.join(bronze_dir, entity_name)
        os.makedirs(dest_entity_dir, exist_ok=True)
        dest_path = os.path.join(dest_entity_dir, f"{entity_name}_bronze.csv")
        
        if not os.path.exists(src_path):
            logger.warning(f"File {src_path} not found. Skipping.")
            continue
            
        with open(src_path, "r", encoding="utf-8") as f_in:
            header = f_in.readline().strip().split(",")
            rows = [line.strip().split(",") for line in f_in if line.strip()]
            
        # Add bronze audit metadata
        bronze_header = header + ["_ingested_at", "_source_file", "_batch_id"]
        
        with open(dest_path, "w", encoding="utf-8") as f_out:
            f_out.write(",".join(bronze_header) + "\n")
            for r in rows:
                row_extended = r + [ingest_time, filename, batch_id]
                f_out.write(",".join(row_extended) + "\n")
                
        ingested_counts[filename] = len(rows)
        logger.info(f"Ingested {len(rows):,} records from {filename} into Bronze layer ({dest_path}).")
        
    return {
        "status": "SUCCESS",
        "batch_id": batch_id,
        "counts": ingested_counts
    }

if __name__ == "__main__":
    result = run_bronze_ingestion()
    print("Bronze Ingestion Result:", result)
