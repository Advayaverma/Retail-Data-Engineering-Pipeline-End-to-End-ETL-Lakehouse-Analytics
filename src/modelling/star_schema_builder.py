"""
Star Schema Builder for Gold Lakehouse Analytics.
Assembles Kimball dimensional star schema:
- dim_date (Fiscal & Calendar)
- dim_store (Regional Hierarchy)
- dim_product (Catalog SKUs)
- dim_customer (SCD Type 2 Customer History)
- fact_sales (Granular Sales Fact with Surrogate Keys)
"""

import os
import csv
from src.utils.logger import get_logger
from src.modelling.date_dimension import generate_date_dimension
from src.modelling.scd2_handler import SCD2CustomerHandler


class StarSchemaBuilder:
    """Assembles all dimensional entities and joins facts with surrogate keys."""

    def __init__(self, gold_dir: str):
        self.gold_dir = gold_dir
        self.logger = get_logger("modelling.star_schema")
        self.scd2_handler = SCD2CustomerHandler()

    def build_dim_date(self) -> list:
        """Generate and persist Date Dimension."""
        self.logger.info("Building Date Dimension (dim_date)...")
        date_rows = generate_date_dimension("2021-01-01", "2026-12-31")
        output_path = os.path.join(self.gold_dir, "dim_date", "dim_date.csv")
        self._write_csv(output_path, date_rows)
        self.logger.info(f"Saved {len(date_rows):,} dim_date records to {output_path}")
        return date_rows

    def build_dim_store(self, raw_stores: list) -> list:
        """Build Store Dimension with surrogate keys."""
        self.logger.info("Building Store Dimension (dim_store)...")
        dim_stores = []
        for idx, s in enumerate(raw_stores, start=1):
            dim_stores.append({
                "store_sk": idx,
                "store_id": s["store_id"],
                "store_name": s["store_name"],
                "city": s["city"],
                "state": s["state"],
                "region": s["region"],
                "store_type": s["store_type"],
                "opened_date": s["opened_date"],
            })
        output_path = os.path.join(self.gold_dir, "dim_store", "dim_store.csv")
        self._write_csv(output_path, dim_stores)
        self.logger.info(f"Saved {len(dim_stores):,} dim_store records to {output_path}")
        return dim_stores

    def build_dim_product(self, raw_products: list) -> list:
        """Build Product Dimension with surrogate keys."""
        self.logger.info("Building Product Dimension (dim_product)...")
        dim_products = []
        for idx, p in enumerate(raw_products, start=1):
            dim_products.append({
                "product_sk": idx,
                "product_id": p["product_id"],
                "product_name": p["product_name"],
                "category": p["category"],
                "subcategory": p["subcategory"],
                "brand": p["brand"],
                "unit_cost": float(p["unit_cost"]),
                "recommended_price": float(p["recommended_price"]),
            })
        output_path = os.path.join(self.gold_dir, "dim_product", "dim_product.csv")
        self._write_csv(output_path, dim_products)
        self.logger.info(f"Saved {len(dim_products):,} dim_product records to {output_path}")
        return dim_products

    def build_dim_customer_scd2(self, baseline_customers: list, updates: list = None) -> list:
        """Build Customer Dimension with SCD Type 2 historical versioning."""
        self.logger.info("Building Customer Dimension (dim_customer SCD Type 2)...")
        dim_cust = self.scd2_handler.initialize_dim_customer(baseline_customers)
        if updates:
            dim_cust, expired, new_vers = self.scd2_handler.process_scd2_updates(dim_cust, updates)

        output_path = os.path.join(self.gold_dir, "dim_customer", "dim_customer.csv")
        self._write_csv(output_path, dim_cust)
        self.logger.info(f"Saved {len(dim_cust):,} dim_customer SCD2 records to {output_path}")
        return dim_cust

    def build_fact_sales(
        self,
        silver_txns: list,
        dim_customers: list,
        dim_products: list,
        dim_stores: list
    ) -> list:
        """
        Assemble Fact Sales table, joining with dimensions to attach surrogate keys
        and calculating financial measures.
        """
        self.logger.info("Assembling fact_sales with dimensional surrogate keys...")
        
        # Build fast lookup hash tables for natural -> surrogate keys
        prod_sk_map = {p["product_id"]: (p["product_sk"], p["unit_cost"]) for p in dim_products}
        store_sk_map = {s["store_id"]: s["store_sk"] for s in dim_stores}
        
        # Customer lookup table (with point-in-time matching)
        cust_sk_map = {}
        for c in dim_customers:
            cid = c["customer_id"]
            if cid not in cust_sk_map:
                cust_sk_map[cid] = []
            cust_sk_map[cid].append(c)

        fact_rows = []
        skipped_count = 0

        for idx, t in enumerate(silver_txns, start=1):
            pid = t["product_id"]
            sid = t["store_id"]
            cid = t["customer_id"]
            txn_date = t.get("txn_date", t.get("transaction_timestamp", "")[:10])

            # Resolve Dimension Surrogate Keys
            prod_info = prod_sk_map.get(pid)
            store_sk = store_sk_map.get(sid)

            if not prod_info or not store_sk or cid not in cust_sk_map:
                skipped_count += 1
                continue

            prod_sk, unit_cost = prod_info

            # Point-in-time SCD2 Customer resolution
            customer_sk = -1
            for version in cust_sk_map[cid]:
                if version["effective_start_date"] <= txn_date <= version["effective_end_date"]:
                    customer_sk = version["customer_sk"]
                    break
            if customer_sk == -1:
                # Fallback to current active version
                customer_sk = next((v["customer_sk"] for v in cust_sk_map[cid] if v["is_current"]), cust_sk_map[cid][0]["customer_sk"])

            # Resolve Date Surrogate Key (YYYYMMDD)
            date_sk = int(txn_date.replace("-", ""))

            # Calculate Numerical Measures
            qty = int(t["quantity"])
            price = float(t["unit_price"])
            disc_rate = float(t.get("discount", 0.0))
            gross_sales = round(qty * price, 2)
            disc_amount = round(gross_sales * disc_rate, 2)
            net_sales = round(gross_sales - disc_amount, 2)
            cost_amount = round(qty * unit_cost, 2)
            profit = round(net_sales - cost_amount, 2)
            profit_margin_pct = round((profit / net_sales) * 100.0, 2) if net_sales > 0 else 0.0

            fact_rows.append({
                "sales_sk": idx,
                "transaction_id": t["transaction_id"],
                "date_sk": date_sk,
                "customer_sk": customer_sk,
                "product_sk": prod_sk,
                "store_sk": store_sk,
                "quantity": qty,
                "unit_price": price,
                "gross_sales": gross_sales,
                "discount_rate": disc_rate,
                "discount_amount": disc_amount,
                "net_sales": net_sales,
                "unit_cost": unit_cost,
                "cost_amount": cost_amount,
                "profit": profit,
                "profit_margin_pct": profit_margin_pct,
                "payment_method": t.get("payment_method", "Other"),
            })

        output_path = os.path.join(self.gold_dir, "fact_sales", "fact_sales.csv")
        self._write_csv(output_path, fact_rows)
        self.logger.info(
            f"Successfully built fact_sales with {len(fact_rows):,} rows ({skipped_count} skipped unmapped). "
            f"Saved to: {output_path}"
        )
        return fact_rows

    def _write_csv(self, file_path: str, rows: list):
        """Helper to write records to CSV."""
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        if not rows:
            return
        fieldnames = list(rows[0].keys())
        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
