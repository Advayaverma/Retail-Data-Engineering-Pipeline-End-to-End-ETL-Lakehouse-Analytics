-- ============================================================================
-- Retail Data Engineering Pipeline — Gold Star Schema Analytics Suite (Phase 9)
-- 10 Core Business Queries Answering Executive Retail Questions
-- ============================================================================

-- ----------------------------------------------------------------------------
-- QUESTION 1: Monthly Revenue & Financial Progression
-- Business Question: What is the monthly breakdown of gross sales, net sales, 
--                   cost, net profit, and profit margin across all periods?
-- ----------------------------------------------------------------------------
SELECT 
    d.year_num,
    d.month_num,
    d.month_name,
    COUNT(DISTINCT f.transaction_id) AS total_orders,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.gross_sales), 2) AS gross_revenue,
    ROUND(SUM(f.discount_amount), 2) AS total_discounts,
    ROUND(SUM(f.net_sales), 2) AS net_revenue,
    ROUND(SUM(f.cost_amount), 2) AS total_cost,
    ROUND(SUM(f.profit), 2) AS net_profit,
    ROUND((SUM(f.profit) / SUM(f.net_sales)) * 100.0, 2) AS profit_margin_pct
FROM fact_sales f
INNER JOIN dim_date d ON f.date_sk = d.date_sk
GROUP BY 
    d.year_num,
    d.month_num,
    d.month_name
ORDER BY 
    d.year_num, 
    d.month_num;


-- ----------------------------------------------------------------------------
-- QUESTION 2: Revenue & Margin by Store Region
-- Business Question: How does revenue and profitability distribute across 
--                   the 5 geographic retail regions?
-- ----------------------------------------------------------------------------
SELECT 
    s.region,
    COUNT(DISTINCT s.store_sk) AS store_count,
    COUNT(f.sales_sk) AS total_line_items,
    ROUND(SUM(f.net_sales), 2) AS total_net_revenue,
    ROUND(SUM(f.profit), 2) AS total_profit,
    ROUND((SUM(f.profit) / SUM(f.net_sales)) * 100.0, 2) AS regional_margin_pct,
    ROUND(
        (SUM(f.net_sales) / (SELECT SUM(net_sales) FROM fact_sales)) * 100.0, 2
    ) AS revenue_share_pct
FROM fact_sales f
INNER JOIN dim_store s ON f.store_sk = s.store_sk
GROUP BY 
    s.region
ORDER BY 
    total_net_revenue DESC;


-- ----------------------------------------------------------------------------
-- QUESTION 3: Top 10 Products by Net Revenue & Profitability
-- Business Question: What are the top 10 best-selling SKUs by net sales,
--                   and what margin do they yield?
-- ----------------------------------------------------------------------------
SELECT 
    p.product_id,
    p.product_name,
    p.category,
    p.brand,
    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.net_sales), 2) AS total_net_revenue,
    ROUND(SUM(f.profit), 2) AS total_profit,
    ROUND((SUM(f.profit) / SUM(f.net_sales)) * 100.0, 2) AS product_margin_pct
FROM fact_sales f
INNER JOIN dim_product p ON f.product_sk = p.product_sk
GROUP BY 
    p.product_id,
    p.product_name,
    p.category,
    p.brand
ORDER BY 
    total_net_revenue DESC
LIMIT 10;


-- ----------------------------------------------------------------------------
-- QUESTION 4: Top 10 Customers by Lifetime Spend
-- Business Question: Who are our highest-value individual customers, what is 
--                   their current segment, and how many orders have they placed?
-- ----------------------------------------------------------------------------
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.city,
    c.state,
    COUNT(DISTINCT f.transaction_id) AS total_orders,
    SUM(f.quantity) AS total_units_bought,
    ROUND(SUM(f.net_sales), 2) AS lifetime_spend
FROM fact_sales f
INNER JOIN dim_customer c ON f.customer_sk = c.customer_sk
WHERE c.is_current = 1  -- Current contact & tier information
GROUP BY 
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.city,
    c.state
ORDER BY 
    lifetime_spend DESC
LIMIT 10;


-- ----------------------------------------------------------------------------
-- QUESTION 5: Customer Lifetime Value (CLV) & Segment Metrics
-- Business Question: What is the average CLV and purchase frequency across
--                   customer segments (VIP, Premium, Corporate, Standard)?
-- ----------------------------------------------------------------------------
WITH CustomerSpend AS (
    SELECT 
        c.customer_id,
        c.customer_segment,
        COUNT(DISTINCT f.transaction_id) AS order_count,
        SUM(f.net_sales) AS customer_total_spend
    FROM fact_sales f
    INNER JOIN dim_customer c ON f.customer_sk = c.customer_sk
    WHERE c.is_current = 1
    GROUP BY 
        c.customer_id,
        c.customer_segment
)
SELECT 
    customer_segment,
    COUNT(customer_id) AS customer_count,
    ROUND(AVG(order_count), 2) AS avg_orders_per_customer,
    ROUND(AVG(customer_total_spend), 2) AS avg_clv,
    ROUND(SUM(customer_total_spend), 2) AS total_segment_revenue
FROM CustomerSpend
GROUP BY 
    customer_segment
ORDER BY 
    avg_clv DESC;


-- ----------------------------------------------------------------------------
-- QUESTION 6: Month-over-Month (MoM) Growth Rate & Variance
-- Business Question: What is the MoM percentage growth in net revenue,
--                   using the LAG() window function?
-- ----------------------------------------------------------------------------
WITH MonthlyTotals AS (
    SELECT 
        d.year_num || '-' || SUBSTR('0' || d.month_num, -2) AS year_month,
        ROUND(SUM(f.net_sales), 2) AS current_net_sales
    FROM fact_sales f
    INNER JOIN dim_date d ON f.date_sk = d.date_sk
    GROUP BY 
        d.year_num,
        d.month_num
),
MoMCalculations AS (
    SELECT 
        year_month,
        current_net_sales,
        LAG(current_net_sales, 1) OVER (ORDER BY year_month) AS previous_net_sales
    FROM MonthlyTotals
)
SELECT 
    year_month,
    current_net_sales,
    COALESCE(previous_net_sales, 0.00) AS previous_net_sales,
    ROUND(current_net_sales - COALESCE(previous_net_sales, current_net_sales), 2) AS absolute_change,
    CASE 
        WHEN previous_net_sales IS NULL OR previous_net_sales = 0 THEN 0.00
        ELSE ROUND(((current_net_sales - previous_net_sales) / previous_net_sales) * 100.0, 2)
    END AS mom_growth_pct
FROM MoMCalculations
ORDER BY 
    year_month;


-- ----------------------------------------------------------------------------
-- QUESTION 7: Average Order Value (AOV) by Store Type & Fiscal Quarter
-- Business Question: How does Average Order Value vary by store format
--                   (Flagship, Supercenter, Express, Mall Outlet) over fiscal periods?
-- ----------------------------------------------------------------------------
SELECT 
    d.fiscal_year,
    d.fiscal_quarter,
    s.store_type,
    COUNT(DISTINCT f.transaction_id) AS total_orders,
    ROUND(SUM(f.net_sales), 2) AS net_sales,
    ROUND(SUM(f.net_sales) / COUNT(DISTINCT f.transaction_id), 2) AS average_order_value
FROM fact_sales f
INNER JOIN dim_date d ON f.date_sk = d.date_sk
INNER JOIN dim_store s ON f.store_sk = s.store_sk
GROUP BY 
    d.fiscal_year,
    d.fiscal_quarter,
    s.store_type
ORDER BY 
    d.fiscal_year, 
    d.fiscal_quarter, 
    average_order_value DESC;


-- ----------------------------------------------------------------------------
-- QUESTION 8: Product Category & Subcategory Profitability Breakdown
-- Business Question: What is the profitability margin and revenue contribution
--                   of each product category and subcategory hierarchy?
-- ----------------------------------------------------------------------------
SELECT 
    p.category,
    p.subcategory,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.net_sales), 2) AS net_revenue,
    ROUND(SUM(f.cost_amount), 2) AS total_cost,
    ROUND(SUM(f.profit), 2) AS total_profit,
    ROUND((SUM(f.profit) / SUM(f.net_sales)) * 100.0, 2) AS profit_margin_pct
FROM fact_sales f
INNER JOIN dim_product p ON f.product_sk = p.product_sk
GROUP BY 
    p.category,
    p.subcategory
ORDER BY 
    p.category, 
    net_revenue DESC;


-- ----------------------------------------------------------------------------
-- QUESTION 9: Store Performance Ranking Within Region
-- Business Question: Rank each store within its geographic territory
--                   using DENSE_RANK() by total revenue.
-- ----------------------------------------------------------------------------
WITH StoreRevenue AS (
    SELECT 
        s.region,
        s.store_name,
        s.city,
        s.state,
        s.store_type,
        COUNT(DISTINCT f.transaction_id) AS total_orders,
        ROUND(SUM(f.net_sales), 2) AS net_revenue,
        ROUND(SUM(f.profit), 2) AS total_profit,
        DENSE_RANK() OVER (
            PARTITION BY s.region 
            ORDER BY SUM(f.net_sales) DESC
        ) AS intra_regional_rank
    FROM fact_sales f
    INNER JOIN dim_store s ON f.store_sk = s.store_sk
    GROUP BY 
        s.region,
        s.store_name,
        s.city,
        s.state,
        s.store_type
)
SELECT 
    region,
    intra_regional_rank,
    store_name,
    city,
    state,
    store_type,
    total_orders,
    net_revenue,
    total_profit
FROM StoreRevenue
WHERE intra_regional_rank <= 3
ORDER BY 
    region, 
    intra_regional_rank;


-- ----------------------------------------------------------------------------
-- QUESTION 10: Repeat Customer Rate & Order Frequency Distribution
-- Business Question: What percentage of our customer base are repeat buyers
--                   (purchased more than 1 order), and what is the retention rate?
-- ----------------------------------------------------------------------------
WITH CustomerOrderCounts AS (
    SELECT 
        c.customer_id,
        COUNT(DISTINCT f.transaction_id) AS order_count
    FROM fact_sales f
    INNER JOIN dim_customer c ON f.customer_sk = c.customer_sk
    GROUP BY 
        c.customer_id
)
SELECT 
    COUNT(customer_id) AS total_active_customers,
    SUM(CASE WHEN order_count = 1 THEN 1 ELSE 0 END) AS one_time_buyers,
    SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) AS repeat_buyers,
    ROUND(
        (CAST(SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(customer_id)) * 100.0, 2
    ) AS repeat_customer_rate_pct
FROM CustomerOrderCounts;
