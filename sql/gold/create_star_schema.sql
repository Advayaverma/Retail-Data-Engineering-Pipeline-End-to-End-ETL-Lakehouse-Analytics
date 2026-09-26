-- ============================================================================
-- Retail Data Engineering Pipeline — Kimball Dimensional Star Schema (Phase 8)
-- Fact Table: fact_sales (grain: line item per transaction)
-- Dimension Tables: dim_date, dim_customer (SCD2), dim_product, dim_store
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS retail_gold;

-- ----------------------------------------------------------------------------
-- 1. DATE DIMENSION (dim_date)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS retail_gold.dim_date CASCADE;

CREATE TABLE retail_gold.dim_date (
    date_sk         INT PRIMARY KEY,              -- Format: YYYYMMDD (e.g. 20250615)
    full_date       DATE NOT NULL UNIQUE,
    day_of_week     INT NOT NULL,                 -- 1 (Monday) to 7 (Sunday)
    day_name        VARCHAR(16) NOT NULL,         -- 'Monday', 'Tuesday', ...
    day_of_month    INT NOT NULL,                 -- 1 to 31
    day_of_year     INT NOT NULL,                 -- 1 to 366
    week_of_year    INT NOT NULL,                 -- 1 to 53
    month_num       INT NOT NULL,                 -- 1 to 12
    month_name      VARCHAR(16) NOT NULL,         -- 'January', ...
    quarter_num     INT NOT NULL,                 -- 1 to 4
    quarter_name    VARCHAR(8) NOT NULL,          -- 'Q1', 'Q2', 'Q3', 'Q4'
    year_num        INT NOT NULL,                 -- e.g. 2024, 2025, 2026
    is_weekend      BOOLEAN NOT NULL,
    fiscal_quarter  VARCHAR(8) NOT NULL,          -- 'FQ1', 'FQ2', ...
    fiscal_year     INT NOT NULL
);

-- ----------------------------------------------------------------------------
-- 2. STORE DIMENSION (dim_store)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS retail_gold.dim_store CASCADE;

CREATE TABLE retail_gold.dim_store (
    store_sk        INT PRIMARY KEY,              -- Surrogate Key
    store_id        VARCHAR(16) NOT NULL UNIQUE,  -- Natural Key
    store_name      VARCHAR(128) NOT NULL,
    city            VARCHAR(64)  NOT NULL,
    state           VARCHAR(8)   NOT NULL,
    region          VARCHAR(32)  NOT NULL,
    store_type      VARCHAR(32)  NOT NULL,
    opened_date     DATE         NOT NULL
);

-- ----------------------------------------------------------------------------
-- 3. PRODUCT DIMENSION (dim_product - SCD Type 1)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS retail_gold.dim_product CASCADE;

CREATE TABLE retail_gold.dim_product (
    product_sk        INT PRIMARY KEY,            -- Surrogate Key
    product_id        VARCHAR(16) NOT NULL UNIQUE,-- Natural Key
    product_name      VARCHAR(128) NOT NULL,
    category          VARCHAR(64)  NOT NULL,
    subcategory       VARCHAR(64)  NOT NULL,
    brand             VARCHAR(64)  NOT NULL,
    unit_cost         NUMERIC(10, 2) NOT NULL,
    recommended_price NUMERIC(10, 2) NOT NULL
);

-- ----------------------------------------------------------------------------
-- 4. CUSTOMER DIMENSION (dim_customer - Slowly Changing Dimension Type 2)
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS retail_gold.dim_customer CASCADE;

CREATE TABLE retail_gold.dim_customer (
    customer_sk          INT PRIMARY KEY,         -- Surrogate Key (Incrementing ID per version)
    customer_id          VARCHAR(16) NOT NULL,    -- Natural Key (Non-unique due to historical versions)
    customer_name        VARCHAR(128) NOT NULL,
    email                VARCHAR(128) NOT NULL,
    city                 VARCHAR(64)  NOT NULL,
    state                VARCHAR(8)   NOT NULL,
    signup_date          DATE         NOT NULL,
    customer_segment     VARCHAR(32)  NOT NULL,
    effective_start_date DATE         NOT NULL,   -- SCD2 valid from
    effective_end_date   DATE         NOT NULL,   -- SCD2 valid to (9999-12-31 for current)
    is_current           BOOLEAN      NOT NULL    -- TRUE if current version, FALSE if historical
);

CREATE INDEX idx_dim_cust_natural ON retail_gold.dim_customer (customer_id);
CREATE INDEX idx_dim_cust_current ON retail_gold.dim_customer (is_current);

-- ----------------------------------------------------------------------------
-- 5. FACT SALES (fact_sales)
-- Grain: One record per line item purchased in a transaction
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS retail_gold.fact_sales CASCADE;

CREATE TABLE retail_gold.fact_sales (
    sales_sk            BIGINT PRIMARY KEY,       -- Fact Surrogate Key
    transaction_id      VARCHAR(32) NOT NULL,     -- Degenerate Dimension
    date_sk             INT         NOT NULL REFERENCES retail_gold.dim_date(date_sk),
    customer_sk         INT         NOT NULL REFERENCES retail_gold.dim_customer(customer_sk),
    product_sk          INT         NOT NULL REFERENCES retail_gold.dim_product(product_sk),
    store_sk            INT         NOT NULL REFERENCES retail_gold.dim_store(store_sk),
    
    -- Numerical Measures
    quantity            INT            NOT NULL,
    unit_price          NUMERIC(10, 2) NOT NULL,
    gross_sales         NUMERIC(12, 2) NOT NULL,
    discount_rate       NUMERIC(4, 2)  NOT NULL,
    discount_amount     NUMERIC(12, 2) NOT NULL,
    net_sales           NUMERIC(12, 2) NOT NULL,
    unit_cost           NUMERIC(10, 2) NOT NULL,
    cost_amount         NUMERIC(12, 2) NOT NULL,
    profit              NUMERIC(12, 2) NOT NULL,
    profit_margin_pct   NUMERIC(6, 2)  NOT NULL,
    payment_method      VARCHAR(32)    NOT NULL
);

CREATE INDEX idx_fact_date ON retail_gold.fact_sales (date_sk);
CREATE INDEX idx_fact_customer ON retail_gold.fact_sales (customer_sk);
CREATE INDEX idx_fact_product ON retail_gold.fact_sales (product_sk);
CREATE INDEX idx_fact_store ON retail_gold.fact_sales (store_sk);
