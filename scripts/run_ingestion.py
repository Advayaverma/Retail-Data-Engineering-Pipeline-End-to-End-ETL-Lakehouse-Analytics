#!/usr/bin/env python3
"""
Master Ingestion Pipeline Runner (Phase 3).
Executes extraction across Transactions, Customers, Products, Stores, and Exchange Rates,
tagging all bronze records with unified batch ID and audit metadata.
"""

import os
import sys
import uuid
import time
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger
from src.ingestion.csv_ingestor import CSVIngestor
from src.ingestion.api_ingestor import APIIngestor
from src.ingestion.postgres_ingestor import PostgresIngestor


def run_all_ingestions(batch_id: str = None) -> dict:
    """
    Run the end-to-end ingestion pipeline for all 5 retail sources into Bronze layer.
    """
    start_time = time.time()
    logger = get_logger("pipeline.ingestion_runner")

    if not batch_id:
        batch_id = f"BATCH-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"

    logger.info("=" * 70)
    logger.info(f"STARTING INGESTION PIPELINE — BATCH ID: {batch_id}")
    logger.info("=" * 70)

    raw_dir = os.path.join(PROJECT_ROOT, "data", "raw")
    bronze_base_dir = os.path.join(PROJECT_ROOT, "data", "bronze")

    results = {}

    # 1. Ingest Transactions
    logger.info("--- [1/5] Ingesting Transactions CSV ---")
    txn_ingestor = CSVIngestor(
        source_name="transactions",
        required_columns=["transaction_id", "product_id", "store_id", "transaction_timestamp"]
    )
    txn_count, txn_path = txn_ingestor.ingest_to_bronze(
        source_file_path=os.path.join(raw_dir, "transactions.csv"),
        bronze_output_dir=os.path.join(bronze_base_dir, "transactions"),
        batch_id=batch_id
    )
    results["transactions"] = {"count": txn_count, "path": txn_path}

    # 2. Ingest Products
    logger.info("--- [2/5] Ingesting Products CSV ---")
    prd_ingestor = CSVIngestor(
        source_name="products",
        required_columns=["product_id", "product_name", "category", "unit_cost"]
    )
    prd_count, prd_path = prd_ingestor.ingest_to_bronze(
        source_file_path=os.path.join(raw_dir, "products.csv"),
        bronze_output_dir=os.path.join(bronze_base_dir, "products"),
        batch_id=batch_id
    )
    results["products"] = {"count": prd_count, "path": prd_path}

    # 3. Ingest Stores
    logger.info("--- [3/5] Ingesting Stores CSV ---")
    str_ingestor = CSVIngestor(
        source_name="stores",
        required_columns=["store_id", "store_name", "city", "state", "region"]
    )
    str_count, str_path = str_ingestor.ingest_to_bronze(
        source_file_path=os.path.join(raw_dir, "stores.csv"),
        bronze_output_dir=os.path.join(bronze_base_dir, "stores"),
        batch_id=batch_id
    )
    results["stores"] = {"count": str_count, "path": str_path}

    # 4. Ingest Customers (PostgreSQL with CSV fallback)
    logger.info("--- [4/5] Ingesting Customer Master (PostgreSQL/Fallback) ---")
    cust_ingestor = PostgresIngestor(
        db_config={
            "host": os.getenv("POSTGRES_HOST", "localhost"),
            "port": int(os.getenv("POSTGRES_PORT", 5432)),
            "name": os.getenv("POSTGRES_DB", "retail_dw"),
            "user": os.getenv("POSTGRES_USER", "retail_admin"),
            "password": os.getenv("POSTGRES_PASSWORD", "retail_secure_pass_2027"),
        },
        fallback_csv_path=os.path.join(raw_dir, "customers.csv")
    )
    cust_count, cust_path = cust_ingestor.ingest_to_bronze(
        bronze_output_dir=os.path.join(bronze_base_dir, "customers"),
        batch_id=batch_id
    )
    results["customers"] = {"count": cust_count, "path": cust_path}

    # 5. Ingest Exchange Rates (REST API with resilient fallback)
    logger.info("--- [5/5] Ingesting Currency Exchange Rates (REST API) ---")
    api_ingestor = APIIngestor(
        api_url=os.getenv("EXCHANGE_RATE_API_URL", "http://localhost:8000/api/v1/rates"),
        base_currency="USD",
        max_retries=2,
        timeout_seconds=3
    )
    rate_count, rate_path = api_ingestor.ingest_to_bronze(
        bronze_output_dir=os.path.join(bronze_base_dir, "exchange_rates"),
        batch_id=batch_id
    )
    results["exchange_rates"] = {"count": rate_count, "path": rate_path}

    duration = time.time() - start_time
    logger.info("=" * 70)
    logger.info(f"BRONZE INGESTION COMPLETED IN {duration:.2f} SECONDS")
    logger.info("=" * 70)
    for source, info in results.items():
        logger.info(f"  * {source.upper():<16}: {info['count']:>7,} records -> {info['path']}")
    logger.info("=" * 70)

    return results


if __name__ == "__main__":
    run_all_ingestions()
