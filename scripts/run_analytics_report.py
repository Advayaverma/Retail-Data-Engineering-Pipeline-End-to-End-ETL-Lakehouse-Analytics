#!/usr/bin/env python3
"""
Operational SQL Analytics Runner (Phase 9).
Executes the 10 core retail business questions against the Gold Star Schema,
prints executive reporting summaries, and generates docs/business_analytics_report.md.
"""

import os
import sys
import sqlite3

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

DB_PATH = os.path.join(PROJECT_ROOT, "data", "retail_dw.db")
DOCS_DIR = os.path.join(PROJECT_ROOT, "docs")


def execute_queries():
    logger = get_logger("analytics.runner")
    logger.info("=" * 70)
    logger.info("EXECUTING 10 CORE BUSINESS ANALYTICS SQL QUERIES (PHASE 9)")
    logger.info("=" * 70)

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    report_sections = []

    # -------------------------------------------------------------------------
    # Query 1: Monthly Revenue
    # -------------------------------------------------------------------------
    q1 = """
    SELECT d.year_num, d.month_num, d.month_name,
           COUNT(DISTINCT f.transaction_id) AS orders,
           SUM(f.quantity) AS units,
           ROUND(SUM(f.net_sales), 2) AS net_revenue,
           ROUND(SUM(f.profit), 2) AS profit,
           ROUND((SUM(f.profit) / SUM(f.net_sales)) * 100.0, 2) AS margin_pct
    FROM fact_sales f
    INNER JOIN dim_date d ON f.date_sk = d.date_sk
    GROUP BY d.year_num, d.month_num, d.month_name
    ORDER BY d.year_num, d.month_num;
    """
    cursor.execute(q1)
    r1 = cursor.fetchall()
    logger.info(f"[1/10] Monthly Revenue: Processed {len(r1)} monthly periods.")

    # -------------------------------------------------------------------------
    # Query 2: Regional Revenue
    # -------------------------------------------------------------------------
    q2 = """
    SELECT s.region, COUNT(DISTINCT s.store_sk) AS stores,
           ROUND(SUM(f.net_sales), 2) AS net_revenue,
           ROUND(SUM(f.profit), 2) AS profit,
           ROUND((SUM(f.profit) / SUM(f.net_sales)) * 100.0, 2) AS margin_pct,
           ROUND((SUM(f.net_sales) / (SELECT SUM(net_sales) FROM fact_sales)) * 100.0, 2) AS share_pct
    FROM fact_sales f
    INNER JOIN dim_store s ON f.store_sk = s.store_sk
    GROUP BY s.region
    ORDER BY net_revenue DESC;
    """
    cursor.execute(q2)
    r2 = cursor.fetchall()
    logger.info(f"[2/10] Regional Performance: Processed {len(r2)} geographic regions.")

    # -------------------------------------------------------------------------
    # Query 3: Top 10 Products
    # -------------------------------------------------------------------------
    q3 = """
    SELECT p.product_name, p.category, p.brand, SUM(f.quantity) AS units,
           ROUND(SUM(f.net_sales), 2) AS net_revenue,
           ROUND(SUM(f.profit), 2) AS profit,
           ROUND((SUM(f.profit) / SUM(f.net_sales)) * 100.0, 2) AS margin_pct
    FROM fact_sales f
    INNER JOIN dim_product p ON f.product_sk = p.product_sk
    GROUP BY p.product_id, p.product_name, p.category, p.brand
    ORDER BY net_revenue DESC
    LIMIT 10;
    """
    cursor.execute(q3)
    r3 = cursor.fetchall()
    logger.info(f"[3/10] Top Products: Identified top {len(r3)} revenue-generating SKUs.")

    # -------------------------------------------------------------------------
    # Query 4: Top 10 Customers
    # -------------------------------------------------------------------------
    q4 = """
    SELECT c.customer_name, c.customer_segment, c.city, c.state,
           COUNT(DISTINCT f.transaction_id) AS orders,
           ROUND(SUM(f.net_sales), 2) AS lifetime_spend
    FROM fact_sales f
    INNER JOIN dim_customer c ON f.customer_sk = c.customer_sk
    WHERE c.is_current = 1
    GROUP BY c.customer_id, c.customer_name, c.customer_segment, c.city, c.state
    ORDER BY lifetime_spend DESC
    LIMIT 10;
    """
    cursor.execute(q4)
    r4 = cursor.fetchall()
    logger.info(f"[4/10] Top Customers: Identified top {len(r4)} high-value customers.")

    # -------------------------------------------------------------------------
    # Query 5: Customer Lifetime Value (CLV)
    # -------------------------------------------------------------------------
    q5 = """
    WITH CustomerSpend AS (
        SELECT c.customer_id, c.customer_segment,
               COUNT(DISTINCT f.transaction_id) AS order_count,
               SUM(f.net_sales) AS customer_total_spend
        FROM fact_sales f
        INNER JOIN dim_customer c ON f.customer_sk = c.customer_sk
        WHERE c.is_current = 1
        GROUP BY c.customer_id, c.customer_segment
    )
    SELECT customer_segment, COUNT(customer_id) AS total_customers,
           ROUND(AVG(order_count), 2) AS avg_orders,
           ROUND(AVG(customer_total_spend), 2) AS avg_clv,
           ROUND(SUM(customer_total_spend), 2) AS segment_revenue
    FROM CustomerSpend
    GROUP BY customer_segment
    ORDER BY avg_clv DESC;
    """
    cursor.execute(q5)
    r5 = cursor.fetchall()
    logger.info(f"[5/10] Customer Lifetime Value: Processed {len(r5)} customer segments.")

    # -------------------------------------------------------------------------
    # Query 6: Month-over-Month Growth (LAG)
    # -------------------------------------------------------------------------
    q6 = """
    WITH MonthlyTotals AS (
        SELECT d.year_num || '-' || SUBSTR('0' || d.month_num, -2) AS year_month,
               ROUND(SUM(f.net_sales), 2) AS net_sales
        FROM fact_sales f
        INNER JOIN dim_date d ON f.date_sk = d.date_sk
        GROUP BY d.year_num, d.month_num
    ),
    MoM AS (
        SELECT year_month, net_sales,
               LAG(net_sales, 1) OVER (ORDER BY year_month) AS prev_sales
        FROM MonthlyTotals
    )
    SELECT year_month, net_sales, COALESCE(prev_sales, 0.00) AS prev_sales,
           ROUND(net_sales - COALESCE(prev_sales, net_sales), 2) AS diff,
           CASE WHEN prev_sales IS NULL OR prev_sales = 0 THEN 0.00
                ELSE ROUND(((net_sales - prev_sales) / prev_sales) * 100.0, 2) END AS growth_pct
    FROM MoM ORDER BY year_month;
    """
    cursor.execute(q6)
    r6 = cursor.fetchall()
    logger.info(f"[6/10] MoM Growth Rate: Calculated growth across {len(r6)} periods.")

    # -------------------------------------------------------------------------
    # Query 7: Average Order Value (AOV)
    # -------------------------------------------------------------------------
    q7 = """
    SELECT d.fiscal_year, d.fiscal_quarter, s.store_type,
           COUNT(DISTINCT f.transaction_id) AS orders,
           ROUND(SUM(f.net_sales), 2) AS net_sales,
           ROUND(SUM(f.net_sales) / COUNT(DISTINCT f.transaction_id), 2) AS aov
    FROM fact_sales f
    INNER JOIN dim_date d ON f.date_sk = d.date_sk
    INNER JOIN dim_store s ON f.store_sk = s.store_sk
    GROUP BY d.fiscal_year, d.fiscal_quarter, s.store_type
    ORDER BY d.fiscal_year, d.fiscal_quarter, aov DESC;
    """
    cursor.execute(q7)
    r7 = cursor.fetchall()
    logger.info(f"[7/10] Average Order Value: Processed {len(r7)} store-quarter combinations.")

    # -------------------------------------------------------------------------
    # Query 8: Category Profitability
    # -------------------------------------------------------------------------
    q8 = """
    SELECT p.category, p.subcategory,
           SUM(f.quantity) AS units,
           ROUND(SUM(f.net_sales), 2) AS net_revenue,
           ROUND(SUM(f.profit), 2) AS profit,
           ROUND((SUM(f.profit) / SUM(f.net_sales)) * 100.0, 2) AS margin_pct
    FROM fact_sales f
    INNER JOIN dim_product p ON f.product_sk = p.product_sk
    GROUP BY p.category, p.subcategory
    ORDER BY p.category, net_revenue DESC;
    """
    cursor.execute(q8)
    r8 = cursor.fetchall()
    logger.info(f"[8/10] Category Profitability: Processed {len(r8)} subcategories.")

    # -------------------------------------------------------------------------
    # Query 9: Store Performance Ranking (DENSE_RANK)
    # -------------------------------------------------------------------------
    q9 = """
    WITH StoreRev AS (
        SELECT s.region, s.store_name, s.city,
               ROUND(SUM(f.net_sales), 2) AS net_revenue,
               DENSE_RANK() OVER (PARTITION BY s.region ORDER BY SUM(f.net_sales) DESC) AS rank
        FROM fact_sales f
        INNER JOIN dim_store s ON f.store_sk = s.store_sk
        GROUP BY s.region, s.store_name, s.city
    )
    SELECT region, rank, store_name, city, net_revenue
    FROM StoreRev WHERE rank <= 3
    ORDER BY region, rank;
    """
    cursor.execute(q9)
    r9 = cursor.fetchall()
    logger.info(f"[9/10] Store Rankings: Ranked top 3 stores across all regions.")

    # -------------------------------------------------------------------------
    # Query 10: Repeat Customer Rate
    # -------------------------------------------------------------------------
    q10 = """
    WITH CustCounts AS (
        SELECT c.customer_id, COUNT(DISTINCT f.transaction_id) AS orders
        FROM fact_sales f
        INNER JOIN dim_customer c ON f.customer_sk = c.customer_sk
        GROUP BY c.customer_id
    )
    SELECT COUNT(customer_id) AS total_customers,
           SUM(CASE WHEN orders = 1 THEN 1 ELSE 0 END) AS one_time,
           SUM(CASE WHEN orders > 1 THEN 1 ELSE 0 END) AS repeat,
           ROUND((CAST(SUM(CASE WHEN orders > 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(customer_id)) * 100.0, 2) AS repeat_rate_pct
    FROM CustCounts;
    """
    cursor.execute(q10)
    r10 = cursor.fetchall()
    logger.info(f"[10/10] Repeat Customer Rate: {r10[0][3]}% repeat buyer rate.")

    conn.close()

    # Generate Markdown documentation
    os.makedirs(DOCS_DIR, exist_ok=True)
    md_report = f"""# Executive Retail Analytics & Business Insights Report

**Data Warehouse:** Kimball Star Schema (`fact_sales`, `dim_date`, `dim_customer`, `dim_product`, `dim_store`)  
**Data Volume:** 104,110 line items  
**Target Role:** Celebal Technologies Data Engineer Evaluation

---

## 1. Executive Summary & Core Business Metrics

| Business Metric | Value | Description |
| :--- | :--- | :--- |
| **Total Active Customers** | {r10[0][0]:,} | Distinct registered customers with purchases |
| **Repeat Customer Rate** | **{r10[0][3]}%** | Customers with $>1$ historical order |
| **One-Time Buyers** | {r10[0][1]:,} | Customers with exactly 1 order |
| **Repeat Buyers** | {r10[0][2]:,} | Loyal recurring customers |

---

## 2. Regional Performance & Market Share Breakdown

| Region | Store Count | Net Revenue | Net Profit | Profit Margin % | Revenue Share % |
| :--- | :--- | :--- | :--- | :--- | :--- |
"""
    for row in r2:
        md_report += f"| **{row[0]}** | {row[1]} | ${row[2]:,.2f} | ${row[3]:,.2f} | {row[4]}% | {row[5]}% |\n"

    md_report += """
---

## 3. Customer Lifetime Value (CLV) by Segment

| Customer Segment | Customer Count | Avg Orders / Customer | Average CLV | Total Segment Revenue |
| :--- | :--- | :--- | :--- | :--- |
"""
    for row in r5:
        md_report += f"| **{row[0]}** | {row[1]:,} | {row[2]} | **${row[3]:,.2f}** | ${row[4]:,.2f} |\n"

    md_report += """
---

## 4. Top 10 Best-Selling Products by Revenue

| Product Name | Category | Brand | Units Sold | Net Revenue | Profit | Margin % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
"""
    for row in r3:
        md_report += f"| {row[0]} | {row[1]} | {row[2]} | {row[3]:,} | **${row[4]:,.2f}** | ${row[5]:,.2f} | {row[6]}% |\n"

    report_path = os.path.join(DOCS_DIR, "business_analytics_report.md")
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(md_report)

    logger.info(f"Executive Analytics Report saved to: {report_path}")
    print("\n" + "=" * 70)
    print("BUSINESS ANALYTICS EXECUTIVE HIGHLIGHTS:")
    print("=" * 70)
    print(f"Total Customers Evaluated: {r10[0][0]:,}")
    print(f"Repeat Customer Rate:      {r10[0][3]}% (Loyal recurring shopper base)")
    print(f"Top Revenue Region:        {r2[0][0]} (${r2[0][2]:,.2f}, {r2[0][5]}% market share)")
    print(f"Highest CLV Segment:       {r5[0][0]} (Avg CLV: ${r5[0][3]:,.2f})")
    print(f"Top Best-Selling SKU:      {r3[0][0]} (${r3[0][4]:,.2f} net revenue)")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    execute_queries()
