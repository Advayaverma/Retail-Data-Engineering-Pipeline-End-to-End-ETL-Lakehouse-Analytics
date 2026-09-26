# Databricks notebook source
# MAGIC %md
# MAGIC # 03 - Gold Star Schema Dimensional Modelling Notebook
# MAGIC ## Medallion Architecture: Silver to Gold Dimensional Warehousing & Kimball Star Schema
# MAGIC 
# MAGIC **Author:** Retail Data Engineering Pipeline  
# MAGIC **Target Role:** Celebal Technologies Data Engineer Evaluation  
# MAGIC **Lakehouse Layer:** Gold (Business-level Kimball Star Schema with Surrogate Keys & SCD Type 2)
# MAGIC 
# MAGIC ### Objectives:
# MAGIC 1. Ingest clean Silver tables.
# MAGIC 2. Build Date Dimension (`dim_date`) covering all relevant historical & operational horizons.
# MAGIC 3. Process Customer Dimension with SCD Type 2 history tracking (`dim_customer`).
# MAGIC 4. Generate Product & Store dimensions (`dim_product`, `dim_store`).
# MAGIC 5. Construct Fact Sales table (`fact_sales`) with surrogate keys, grain alignment, and metrics.

# COMMAND ----------
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.databricks_utils import is_running_on_databricks, get_lakehouse_path
from src.utils.logger import get_logger
from scripts.build_star_schema import run_build_star_schema

logger = get_logger("Notebook_03_Gold")

# COMMAND ----------
# MAGIC %md
# MAGIC ### Build Kimball Star Schema

# COMMAND ----------
def run_gold_star_schema():
    logger.info("Executing Gold Star Schema and SCD Type 2 dimensional modeling...")
    metrics = run_build_star_schema()
    return {
        "status": "SUCCESS",
        "gold_metrics": metrics
    }

if __name__ == "__main__":
    result = run_gold_star_schema()
    print("Gold Star Schema Result:", result)
