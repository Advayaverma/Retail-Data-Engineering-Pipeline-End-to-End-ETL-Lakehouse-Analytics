-- ============================================================================
-- Retail Data Engineering Pipeline — Advanced Analytical SQL Queries (Phase 4)
-- Demonstrating JOINs, GROUP BY, HAVING, Subqueries, CTEs, and Window Functions
-- ============================================================================

-- ----------------------------------------------------------------------------
-- QUERY 1: High-Performing Stores Analysis
-- Concepts: Multiple INNER JOINs, Aggregations, GROUP BY, HAVING filter
-- Business Question: Which stores generated more than $100,000 in gross revenue,
--                   and what is their average item price and transaction count?
-- ----------------------------------------------------------------------------
SELECT 
    s.store_id,
    s.store_name,
    s.city,
    s.region,
    COUNT(t.id) AS total_transactions,
    SUM(t.quantity) AS total_units_sold,
    ROUND(AVG(t.unit_price), 2) AS avg_unit_price,
    ROUND(SUM(t.quantity * t.unit_price * (1 - t.discount)), 2) AS net_revenue
FROM retail_source.transactions t
INNER JOIN retail_source.stores s 
    ON t.store_id = s.store_id
GROUP BY 
    s.store_id,
    s.store_name,
    s.city,
    s.region
HAVING 
    SUM(t.quantity * t.unit_price * (1 - t.discount)) > 100000.00
ORDER BY 
    net_revenue DESC;


-- ----------------------------------------------------------------------------
-- QUERY 2: High-Value Customer Identification
-- Concepts: Subqueries in WHERE clause, Scalar Aggregate Subquery, INNER JOIN
-- Business Question: Find all customers whose total lifetime spend exceeds 
--                   2x the overall customer average spend.
-- ----------------------------------------------------------------------------
SELECT 
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.city,
    c.state,
    ROUND(SUM(t.quantity * t.unit_price * (1 - t.discount)), 2) AS customer_lifetime_spend
FROM retail_source.customers c
INNER JOIN retail_source.transactions t 
    ON c.customer_id = t.customer_id
GROUP BY 
    c.customer_id,
    c.customer_name,
    c.customer_segment,
    c.city,
    c.state
HAVING 
    SUM(t.quantity * t.unit_price * (1 - t.discount)) > (
        -- Subquery calculating 2x average spend across all active customers
        SELECT 2.0 * AVG(customer_total)
        FROM (
            SELECT SUM(t2.quantity * t2.unit_price * (1 - t2.discount)) AS customer_total
            FROM retail_source.transactions t2
            WHERE t2.customer_id IS NOT NULL AND t2.customer_id != ''
            GROUP BY t2.customer_id
        ) sub
    )
ORDER BY 
    customer_lifetime_spend DESC
LIMIT 50;


-- ----------------------------------------------------------------------------
-- QUERY 3: Category Top Performers
-- Concepts: CTE (Common Table Expression), Window Functions (DENSE_RANK() OVER PARTITION)
-- Business Question: Rank the top 3 highest revenue-generating products within 
--                   each category.
-- ----------------------------------------------------------------------------
WITH ProductRevenue AS (
    SELECT 
        p.category,
        p.subcategory,
        p.product_id,
        p.product_name,
        p.brand,
        ROUND(SUM(t.quantity * t.unit_price * (1 - t.discount)), 2) AS total_revenue,
        SUM(t.quantity) AS total_units_sold,
        DENSE_RANK() OVER (
            PARTITION BY p.category 
            ORDER BY SUM(t.quantity * t.unit_price * (1 - t.discount)) DESC
        ) AS category_rank
    FROM retail_source.transactions t
    INNER JOIN retail_source.products p 
        ON t.product_id = p.product_id
    GROUP BY 
        p.category,
        p.subcategory,
        p.product_id,
        p.product_name,
        p.brand
)
SELECT 
    category,
    category_rank,
    product_name,
    brand,
    total_units_sold,
    total_revenue
FROM ProductRevenue
WHERE category_rank <= 3
ORDER BY 
    category, 
    category_rank;


-- ----------------------------------------------------------------------------
-- QUERY 4: Month-over-Month Revenue Growth & Percentage Variance
-- Concepts: Multiple CTEs, Date Truncation, Window Functions (LAG() OVER)
-- Business Question: Calculate monthly net revenue, previous month revenue, 
--                   absolute growth, and Month-over-Month (MoM) growth percentage.
-- ----------------------------------------------------------------------------
WITH MonthlySales AS (
    SELECT 
        SUBSTR(t.transaction_timestamp, 1, 7) AS sales_month,
        ROUND(SUM(t.quantity * t.unit_price * (1 - t.discount)), 2) AS current_month_revenue,
        COUNT(DISTINCT t.transaction_id) AS monthly_order_count
    FROM retail_source.transactions t
    WHERE t.transaction_timestamp NOT LIKE '%INVALID%'
      AND t.unit_price > 0
      AND t.quantity > 0
    GROUP BY 
        SUBSTR(t.transaction_timestamp, 1, 7)
),
MonthlyGrowth AS (
    SELECT 
        sales_month,
        current_month_revenue,
        monthly_order_count,
        LAG(current_month_revenue, 1) OVER (ORDER BY sales_month) AS previous_month_revenue
    FROM MonthlySales
)
SELECT 
    sales_month,
    current_month_revenue,
    COALESCE(previous_month_revenue, 0.00) AS previous_month_revenue,
    ROUND(current_month_revenue - COALESCE(previous_month_revenue, current_month_revenue), 2) AS absolute_change,
    CASE 
        WHEN previous_month_revenue IS NULL OR previous_month_revenue = 0 THEN 0.00
        ELSE ROUND(((current_month_revenue - previous_month_revenue) / previous_month_revenue) * 100.0, 2)
    END AS mom_growth_pct
FROM MonthlyGrowth
ORDER BY 
    sales_month;


-- ----------------------------------------------------------------------------
-- QUERY 5: Cumulative Running Total Revenue by Region
-- Concepts: Window Function with Cumulative Frame (SUM() OVER ORDER BY)
-- Business Question: Track running cumulative revenue progression by region.
-- ----------------------------------------------------------------------------
WITH RegionalMonthly AS (
    SELECT 
        s.region,
        SUBSTR(t.transaction_timestamp, 1, 7) AS sales_month,
        ROUND(SUM(t.quantity * t.unit_price * (1 - t.discount)), 2) AS monthly_revenue
    FROM retail_source.transactions t
    INNER JOIN retail_source.stores s 
        ON t.store_id = s.store_id
    WHERE t.unit_price > 0 AND t.quantity > 0
    GROUP BY 
        s.region,
        SUBSTR(t.transaction_timestamp, 1, 7)
)
SELECT 
    region,
    sales_month,
    monthly_revenue,
    ROUND(SUM(monthly_revenue) OVER (
        PARTITION BY region 
        ORDER BY sales_month
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ), 2) AS cumulative_running_revenue
FROM RegionalMonthly
ORDER BY 
    region, 
    sales_month;
