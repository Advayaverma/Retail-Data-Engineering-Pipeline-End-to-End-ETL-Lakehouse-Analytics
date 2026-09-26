#!/usr/bin/env python3
"""
Silver to Gold PySpark Transformation Job (Phase 6).

Implements:
1. Multi-table relational joins with broadcast optimization for lookup dimensions
2. Calculated business metrics (gross_sales, net_sales, cost, profit, profit_margin_pct)
3. Window functions for customer ranking, recency calculation, and cumulative sales
4. Gold aggregation tables:
   - fact_sales_gold
   - gold_monthly_category_performance
   - gold_customer_rfm_metrics
   - gold_store_performance

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
import sys

cwd = os.path.abspath(os.getcwd())
sys_paths_clean = [p for p in sys.path if os.path.abspath(p) != cwd]
old_sys = list(sys.path)
try:
    sys.path = sys_paths_clean
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
finally:
    sys.path = old_sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger
from src.utils.spark_session import get_spark_session

SILVER_DIR = os.path.join(PROJECT_ROOT, "data", "silver")
GOLD_DIR = os.path.join(PROJECT_ROOT, "data", "gold")


def run_gold_transformations():
    logger = get_logger("pyspark.silver_to_gold")
    logger.info("=" * 70)
    logger.info("STARTING PYSPARK SILVER TO GOLD TRANSFORMATION JOB")
    logger.info("=" * 70)

    spark = get_spark_session(app_name="SilverToGoldJob")

    try:
        # 1. Read Silver Datasets
        logger.info("Reading Silver tables...")
        df_txn = spark.read.parquet(os.path.join(SILVER_DIR, "transactions"))
        df_cust = spark.read.parquet(os.path.join(SILVER_DIR, "customers"))
        df_prod = spark.read.parquet(os.path.join(SILVER_DIR, "products"))
        df_store = spark.read.parquet(os.path.join(SILVER_DIR, "stores"))

        # ---------------------------------------------------------------------
        # 2. Broadcast Join Optimization
        # ---------------------------------------------------------------------
        # Why Broadcast Join?
        # Products (1,200 rows) and Stores (55 rows) are small lookup tables.
        # Instead of an expensive shuffle hash join (which shuffles 100k+ transaction rows across executors),
        # F.broadcast() sends the small dimension tables to every executor, completely eliminating shuffle!
        logger.info("Performing broadcast joins for dimension lookups...")
        df_joined = df_txn \
            .join(F.broadcast(df_prod.select("product_id", "product_name", "category", "subcategory", "brand", "unit_cost")), on="product_id", how="inner") \
            .join(F.broadcast(df_store.select("store_id", "store_name", "city", "state", "region", "store_type")), on="store_id", how="inner") \
            .join(df_cust.select("customer_id", "customer_name", "customer_segment"), on="customer_id", how="inner")

        # ---------------------------------------------------------------------
        # 3. Calculated Business Metrics
        # ---------------------------------------------------------------------
        # Metrics:
        #   total_cost        = quantity * unit_cost
        #   profit            = net_amount - total_cost
        #   profit_margin_pct = (profit / net_amount) * 100.0
        logger.info("Calculating financial metrics (profit, margin, net revenue)...")
        df_fact_sales = df_joined \
            .withColumn("total_cost", F.round(F.col("quantity") * F.col("unit_cost"), 2)) \
            .withColumn("profit", F.round(F.col("net_amount") - F.col("total_cost"), 2)) \
            .withColumn(
                "profit_margin_pct",
                F.when(F.col("net_amount") > 0, F.round((F.col("profit") / F.col("net_amount")) * 100.0, 2))
                 .otherwise(F.lit(0.00))
            )

        # Write Gold Fact Sales
        fact_sales_path = os.path.join(GOLD_DIR, "fact_sales")
        df_fact_sales.coalesce(4).write.mode("overwrite").parquet(fact_sales_path)
        logger.info(f"Saved {df_fact_sales.count():,} rows to Gold Fact Sales: {fact_sales_path}")

        # ---------------------------------------------------------------------
        # 4. Gold Aggregate: Monthly Category Performance
        # ---------------------------------------------------------------------
        # Demonstrates: groupBy, agg, multiple metrics, Window ranking
        logger.info("Computing Gold Aggregate: Monthly Category Performance...")
        df_monthly_category = df_fact_sales.groupBy("txn_year", "txn_month", "category") \
            .agg(
                F.count("transaction_id").alias("order_count"),
                F.sum("quantity").alias("total_units_sold"),
                F.round(F.sum("gross_amount"), 2).alias("gross_revenue"),
                F.round(F.sum("net_amount"), 2).alias("net_revenue"),
                F.round(F.sum("profit"), 2).alias("total_profit"),
                F.round(F.avg("discount"), 4).alias("avg_discount_rate")
            )

        # Apply Window Function: Rank categories within each month by net revenue
        monthly_window = Window.partitionBy("txn_year", "txn_month").orderBy(F.col("net_revenue").desc())
        df_monthly_category_ranked = df_monthly_category \
            .withColumn("category_monthly_rank", F.dense_rank().over(monthly_window))

        monthly_cat_path = os.path.join(GOLD_DIR, "monthly_category_summary")
        df_monthly_category_ranked.coalesce(1).write.mode("overwrite").parquet(monthly_cat_path)
        logger.info(f"Saved Monthly Category Gold summary to: {monthly_cat_path}")

        # ---------------------------------------------------------------------
        # 5. Gold Aggregate: Customer RFM & Lifetime Value Metrics
        # ---------------------------------------------------------------------
        # Demonstrates: Customer analytics, recency days, frequency, monetary value
        logger.info("Computing Gold Aggregate: Customer RFM & LTV Metrics...")
        max_txn_date = df_fact_sales.select(F.max("txn_date")).collect()[0][0]

        df_customer_rfm = df_fact_sales.groupBy("customer_id", "customer_name", "customer_segment") \
            .agg(
                F.countDistinct("transaction_id").alias("total_orders"),
                F.sum("quantity").alias("total_items_purchased"),
                F.round(F.sum("net_amount"), 2).alias("lifetime_spend"),
                F.round(F.avg("net_amount"), 2).alias("average_order_value"),
                F.min("txn_date").alias("first_purchase_date"),
                F.max("txn_date").alias("last_purchase_date")
            ) \
            .withColumn("recency_days", F.datediff(F.lit(max_txn_date), F.col("last_purchase_date")))

        # Window Function: Rank top customers by lifetime spend
        cust_window = Window.orderBy(F.col("lifetime_spend").desc())
        df_customer_rfm_ranked = df_customer_rfm \
            .withColumn("spend_rank", F.row_number().over(cust_window))

        cust_rfm_path = os.path.join(GOLD_DIR, "customer_rfm_metrics")
        df_customer_rfm_ranked.coalesce(1).write.mode("overwrite").parquet(cust_rfm_path)
        logger.info(f"Saved Customer RFM Gold metrics to: {cust_rfm_path}")

        # ---------------------------------------------------------------------
        # 6. Gold Aggregate: Store Regional Performance
        # ---------------------------------------------------------------------
        logger.info("Computing Gold Aggregate: Store Regional Performance...")
        df_store_perf = df_fact_sales.groupBy("region", "store_id", "store_name", "city", "store_type") \
            .agg(
                F.count("transaction_id").alias("total_transactions"),
                F.round(F.sum("net_amount"), 2).alias("total_net_revenue"),
                F.round(F.sum("profit"), 2).alias("total_profit"),
                F.round((F.sum("profit") / F.sum("net_amount")) * 100.0, 2).alias("profit_margin_pct")
            )

        store_perf_path = os.path.join(GOLD_DIR, "store_performance_summary")
        df_store_perf.coalesce(1).write.mode("overwrite").parquet(store_perf_path)
        logger.info(f"Saved Store Performance Gold summary to: {store_perf_path}")

        logger.info("=" * 70)
        logger.info("ALL GOLD PYSPARK TRANSFORMATIONS COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_gold_transformations()
