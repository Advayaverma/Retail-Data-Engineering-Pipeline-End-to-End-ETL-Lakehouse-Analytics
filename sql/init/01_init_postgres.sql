-- ============================================================================
-- Retail Data Engineering Pipeline — PostgreSQL Relational Schema (Phase 4)
-- Schemas: retail_source (OLTP staging), retail_dw (Analytical Warehouse)
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS retail_source;
CREATE SCHEMA IF NOT EXISTS retail_dw;

-- ----------------------------------------------------------------------------
-- 1. STORES TABLE
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS retail_source.stores CASCADE;

CREATE TABLE retail_source.stores (
    store_id     VARCHAR(16) PRIMARY KEY,
    store_name   VARCHAR(128) NOT NULL,
    city         VARCHAR(64)  NOT NULL,
    state        VARCHAR(8)   NOT NULL,
    region       VARCHAR(32)  NOT NULL,
    store_type   VARCHAR(32)  NOT NULL,
    opened_date  DATE         NOT NULL,
    created_at   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE retail_source.stores IS 'Physical retail store branch metadata and regional classification';

-- ----------------------------------------------------------------------------
-- 2. PRODUCTS TABLE
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS retail_source.products CASCADE;

CREATE TABLE retail_source.products (
    product_id        VARCHAR(16) PRIMARY KEY,
    product_name      VARCHAR(128) NOT NULL,
    category          VARCHAR(64)  NOT NULL,
    subcategory       VARCHAR(64)  NOT NULL,
    brand             VARCHAR(64)  NOT NULL,
    unit_cost         NUMERIC(10, 2) NOT NULL CHECK (unit_cost > 0),
    recommended_price NUMERIC(10, 2) NOT NULL CHECK (recommended_price >= unit_cost),
    created_at        TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE retail_source.products IS 'Catalog SKUs with category hierarchy and cost/pricing rules';

-- ----------------------------------------------------------------------------
-- 3. CUSTOMERS TABLE
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS retail_source.customers CASCADE;

CREATE TABLE retail_source.customers (
    customer_id      VARCHAR(16) PRIMARY KEY,
    customer_name    VARCHAR(128) NOT NULL,
    email            VARCHAR(128) NOT NULL,
    city             VARCHAR(64)  NOT NULL,
    state            VARCHAR(8)   NOT NULL,
    signup_date      DATE         NOT NULL,
    customer_segment VARCHAR(32)  NOT NULL CHECK (customer_segment IN ('Standard', 'Premium', 'VIP', 'Corporate')),
    created_at       TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE retail_source.customers IS 'Customer master records with demographic segmentation';

-- ----------------------------------------------------------------------------
-- 4. TRANSACTIONS TABLE
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS retail_source.transactions CASCADE;

CREATE TABLE retail_source.transactions (
    id                    BIGSERIAL PRIMARY KEY,
    transaction_id        VARCHAR(32)    NOT NULL,
    customer_id           VARCHAR(16),
    product_id            VARCHAR(16)    NOT NULL,
    store_id              VARCHAR(16)    NOT NULL,
    transaction_timestamp TIMESTAMP      NOT NULL,
    quantity              INT            NOT NULL,
    unit_price            NUMERIC(10, 2) NOT NULL,
    discount              NUMERIC(4, 2)  DEFAULT 0.00,
    payment_method        VARCHAR(32)    NOT NULL,
    ingested_at           TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE retail_source.transactions IS 'Raw operational transactions staging table';
