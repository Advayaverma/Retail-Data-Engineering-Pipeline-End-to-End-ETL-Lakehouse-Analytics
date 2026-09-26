#!/usr/bin/env python3
"""
Bronze to Silver PySpark Transformation Job (Phase 6).

Implements:
1. Strict schema enforcement via StructType
2. Explicit type casting (timestamps, numeric decimals, integers)
3. Null handling and deduplication (dropDuplicates on transaction_id)
4. Categorical standardization (payment method normalization via PySpark SQL functions)
5. Metric validation (unit_price > 0, quantity > 0, 0 <= discount <= 1)
6. Repartitioning and optimized Parquet/Delta storage in data/silver/

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
import sys

# Ensure site-packages takes precedence over local pyspark folder
cwd = os.path.abspath(os.getcwd())
sys_paths_clean = [p for p in sys.path if os.path.abspath(p) != cwd]
old_sys = list(sys.path)
try:
    sys.path = sys_paths_clean
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
    )
finally:
    sys.path = old_sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

BRONZE_DIR = os.path.join(PROJECT_ROOT, "data", "bronze")
SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")


def process_transactions_bronze_to_silver(spark, logger):
    """
    Transform Bronze transactions into clean, standardized Silver transactions.
    """
    logger.info("--- Transforming Transactions: Bronze -> Silver ---")
    bronze_path = os.path.join(BRONZE_DIR, "transactions", "transactions_bronze.csv")
    silver_output_path = os.path.join(SILVER_DIR, "transactions")

    # 1. Define Explicit Schema (Schema Enforcement)
    # Why? Schema inference requires a full scan of data and can guess incorrect types.
    raw_schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), False),
        StructField("store_id", StringType(), False),
        StructField("transaction_timestamp", StringType(), True),
        StructField("quantity", StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("discount", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_source_file", StringType(), True),
        StructField("_batch_id", StringType(), True),
    ])

    # Read Bronze data
    df_raw = spark.read \
        .option("header", "true") \
        .schema(raw_schema) \
        .csv(bronze_path)

    raw_count = df_raw.count()
    logger.info(f"Loaded {raw_count:,} raw bronze transactions.")

    # 2. Type Casting & Transformation Pipeline
    # Why select & withColumn? select prunes unnecessary fields; withColumn creates typed metrics.
    df_transformed = df_raw \
        .withColumn("txn_timestamp", F.to_timestamp(F.col("transaction_timestamp"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("quantity", F.col("quantity").cast(IntegerType())) \
        .withColumn("unit_price", F.round(F.col("unit_price").cast(DoubleType()), 2)) \
        .withColumn("discount", F.round(F.col("discount").cast(DoubleType()), 2)) \
        .withColumn("customer_id", F.trim(F.col("customer_id"))) \
        .withColumn("product_id", F.trim(F.col("product_id"))) \
        .withColumn("store_id", F.trim(F.col("store_id"))) \
        .withColumn("raw_payment", F.trim(F.lower(F.col("payment_method"))))

    # 3. Categorical Standardization
    # Why? Payment methods arrive in inconsistent casing ('credit_card', 'CREDIT CARD', 'CC').
    df_standardized = df_transformed.withColumn(
        "payment_method",
        F.when(F.col("raw_payment").isin("credit card", "credit_card", "cc"), F.lit("Credit Card"))
         .when(F.col("raw_payment").isin("debit card", "debit-card", "dc"), F.lit("Debit Card"))
         .when(F.col("raw_payment").isin("upi"), F.lit("UPI"))
         .when(F.col("raw_payment").isin("net banking", "netbanking"), F.lit("Net Banking"))
         .when(F.col("raw_payment").isin("cash"), F.lit("Cash"))
         .otherwise(F.lit("Other"))
    ).drop("raw_payment")

    # 4. Null Handling & Data Quality Filtering
    # Filter out rows with invalid timestamps, negative/zero prices, non-positive quantities, or missing keys
    df_cleaned = df_standardized.filter(
        F.col("transaction_id").isNotNull() & (F.col("transaction_id") != "") &
        F.col("customer_id").isNotNull() & (F.col("customer_id") != "") &
        F.col("txn_timestamp").isNotNull() &
        (F.col("unit_price") > 0.0) &
        (F.col("quantity") > 0) &
        (F.col("discount") >= 0.0) & (F.col("discount") <= 1.0)
    )

    # 5. Deduplication
    # Why dropDuplicates? Eliminate duplicate transaction receipts caused by replay or network retries.
    df_deduped = df_cleaned.dropDuplicates(["transaction_id"])

    # 6. Feature Engineering for Downstream Modeling
    # Derive year, month, day for partitioning and analytical slicing
    df_silver = df_deduped \
        .withColumn("txn_year", F.year(F.col("txn_timestamp"))) \
        .withColumn("txn_month", F.month(F.col("txn_timestamp"))) \
        .withColumn("txn_date", F.to_date(F.col("txn_timestamp"))) \
        .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2)) \
        .withColumn("discount_amount", F.round(F.col("gross_amount") * F.col("discount"), 2)) \
        .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))

    silver_count = df_silver.count()
    quarantined_count = raw_count - silver_count
    logger.info(f"Silver cleaning completed: {silver_count:,} valid records ({quarantined_count:,} invalid/duplicates filtered).")

    # 7. Coalesce & Write to Silver
    # Why coalesce(4)? Avoid small-file problem by consolidating output partitions for local storage.
    df_silver.coalesce(4).write \
        .mode("overwrite") \
        .parquet(silver_output_path)

    logger.info(f"Saved Silver Transactions Parquet dataset to: {silver_output_path}")
    return silver_count


def process_dimensions_bronze_to_silver(spark, logger):
    """Clean and standardize Customers, Products, and Stores into Silver layer."""
    # 1. Customers
    cust_bronze = os.path.join(BRONZE_DIR, "customers", "customers_bronze.csv")
    cust_silver = os.path.join(SILVER_DIR, "customers")
    df_cust = spark.read.option("header", "true").csv(cust_bronze) \
        .withColumn("customer_id", F.trim(F.col("customer_id"))) \
        .withColumn("customer_name", F.initcap(F.trim(F.col("customer_name")))) \
        .withColumn("email", F.lower(F.trim(F.col("email")))) \
        .withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd")) \
        .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != "")) \
        .dropDuplicates(["customer_id"])

    df_cust.coalesce(1).write.mode("overwrite").parquet(cust_silver)
    logger.info(f"Saved {df_cust.count():,} Silver Customers to: {cust_silver}")

    # 2. Products
    prod_bronze = os.path.join(BRONZE_DIR, "products", "products_bronze.csv")
    prod_silver = os.path.join(SILVER_DIR, "products")
    df_prod = spark.read.option("header", "true").csv(prod_bronze) \
        .withColumn("product_id", F.trim(F.col("product_id"))) \
        .withColumn("unit_cost", F.col("unit_cost").cast(DoubleType())) \
        .withColumn("recommended_price", F.col("recommended_price").cast(DoubleType())) \
        .filter(F.col("unit_cost") > 0.0) \
        .dropDuplicates(["product_id"])

    df_prod.coalesce(1).write.mode("overwrite").parquet(prod_silver)
    logger.info(f"Saved {df_prod.count():,} Silver Products to: {prod_silver}")

    # 3. Stores
    store_bronze = os.path.join(BRONZE_DIR, "stores", "stores_bronze.csv")
    store_silver = os.path.join(SILVER_DIR, "stores")
    df_store = spark.read.option("header", "true").csv(store_bronze) \
        .withColumn("store_id", F.trim(F.col("store_id"))) \
        .withColumn("opened_date", F.to_date(F.col("opened_date"), "yyyy-MM-dd")) \
        .dropDuplicates(["store_id"])

    df_store.coalesce(1).write.mode("overwrite").parquet(store_silver)
    logger.info(f"Saved {df_store.count():,} Silver Stores to: {store_silver}")


def run_silver_pipeline():
    logger = get_logger("pyspark.bronze_to_silver")
    logger.info("=" * 70)
    logger.info("STARTING PYSPARK BRONZE TO SILVER PIPELINE JOB")
    logger.info("=" * 70)

    spark = get_spark_session(app_name="BronzeToSilverJob")
    try:
        process_transactions_bronze_to_silver(spark, logger)
        process_dimensions_bronze_to_silver(spark, logger)
        logger.info("Bronze to Silver transformation pipeline completed successfully.")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_silver_pipeline()
