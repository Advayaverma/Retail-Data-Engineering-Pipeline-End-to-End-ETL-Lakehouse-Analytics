-- ============================================================================
-- Retail Data Engineering Pipeline — Relational Indexes (Phase 4)
-- Optimizing lookup latency, foreign key joins, and time-series range scans
-- ============================================================================

-- 1. Transactions Foreign Key & Join Indexes
CREATE INDEX IF NOT EXISTS idx_txn_customer_id 
    ON retail_source.transactions (customer_id);

CREATE INDEX IF NOT EXISTS idx_txn_product_id 
    ON retail_source.transactions (product_id);

CREATE INDEX IF NOT EXISTS idx_txn_store_id 
    ON retail_source.transactions (store_id);

-- 2. Time-series & Range Scan Indexes (Crucial for Partition Pruning & Window Queries)
CREATE INDEX IF NOT EXISTS idx_txn_timestamp 
    ON retail_source.transactions (transaction_timestamp);

-- 3. Composite Indexes for Frequent Multi-attribute Filtering
CREATE INDEX IF NOT EXISTS idx_cust_segment_state 
    ON retail_source.customers (customer_segment, state);

CREATE INDEX IF NOT EXISTS idx_prod_category_subcat 
    ON retail_source.products (category, subcategory);

CREATE INDEX IF NOT EXISTS idx_txn_store_timestamp 
    ON retail_source.transactions (store_id, transaction_timestamp DESC);
