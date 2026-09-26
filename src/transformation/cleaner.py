"""
Silver & Gold Data Cleaning and Transformation Engine.
Provides high-throughput relational data transformations, metric derivations,
and analytical aggregations mirroring PySpark logic for local and distributed lakehouse pipelines.
"""

import os
import csv
from datetime import datetime
from src.utils.logger import get_logger

PAYMENT_NORMALIZATION = {
    "credit card": "Credit Card",
    "credit_card": "Credit Card",
    "cc": "Credit Card",
    "debit card": "Debit Card",
    "debit-card": "Debit Card",
    "dc": "Debit Card",
    "upi": "UPI",
    "net banking": "Net Banking",
    "netbanking": "Net Banking",
    "cash": "Cash",
}


class SilverTransformer:
    """Bronze to Silver cleansing and standardization engine."""

    def __init__(self):
        self.logger = get_logger("transformation.silver")

    def clean_transactions(self, bronze_records: list) -> list:
        """
        Apply strict type casting, deduplication, categorical standardization,
        and derived financial metrics.
        """
        seen_ids = set()
        clean_rows = []

        for r in bronze_records:
            tid = r.get("transaction_id", "").strip()
            cid = r.get("customer_id", "").strip()
            pid = r.get("product_id", "").strip()
            sid = r.get("store_id", "").strip()
            ts_str = r.get("transaction_timestamp", "").strip()

            # Null check & primary key check
            if not tid or not cid or not pid or not sid:
                continue

            # Deduplication
            if tid in seen_ids:
                continue
            seen_ids.add(tid)

            # Timestamp parsing
            try:
                dt = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                continue

            # Numeric parsing & validations
            try:
                qty = int(r.get("quantity", 0))
                price = round(float(r.get("unit_price", 0.0)), 2)
                discount = round(float(r.get("discount", 0.0)), 2)
            except (ValueError, TypeError):
                continue

            if qty <= 0 or price <= 0.0 or discount < 0.0 or discount > 1.0:
                continue

            # Categorical standardization
            raw_pay = r.get("payment_method", "").strip().lower()
            std_payment = PAYMENT_NORMALIZATION.get(raw_pay, "Other")

            # Financial metric derivations
            gross_amount = round(qty * price, 2)
            discount_amount = round(gross_amount * discount, 2)
            net_amount = round(gross_amount - discount_amount, 2)

            clean_rows.append({
                "transaction_id": tid,
                "customer_id": cid,
                "product_id": pid,
                "store_id": sid,
                "transaction_timestamp": ts_str,
                "txn_date": dt.strftime("%Y-%m-%d"),
                "txn_year": dt.year,
                "txn_month": dt.month,
                "quantity": qty,
                "unit_price": price,
                "discount": discount,
                "gross_amount": gross_amount,
                "discount_amount": discount_amount,
                "net_amount": net_amount,
                "payment_method": std_payment,
                "_ingested_at": r.get("_ingested_at", ""),
                "_source_file": r.get("_source_file", ""),
                "_batch_id": r.get("_batch_id", ""),
            })

        self.logger.info(f"Bronze -> Silver: Processed {len(bronze_records):,} rows -> {len(clean_rows):,} clean rows.")
        return clean_rows


class GoldTransformer:
    """Silver to Gold dimensional star schema & business aggregates engine."""

    def __init__(self):
        self.logger = get_logger("transformation.gold")

    def build_fact_sales(self, silver_txns: list, products: list, stores: list, customers: list) -> list:
        """
        Enrich transactions with dimension keys, unit costs, and profit metrics.
        """
        prod_map = {p["product_id"]: p for p in products}
        store_map = {s["store_id"]: s for s in stores}
        cust_map = {c["customer_id"]: c for c in customers}

        facts = []
        for t in silver_txns:
            p = prod_map.get(t["product_id"])
            s = store_map.get(t["store_id"])
            c = cust_map.get(t["customer_id"])

            if not p or not s or not c:
                continue

            unit_cost = float(p["unit_cost"])
            total_cost = round(t["quantity"] * unit_cost, 2)
            profit = round(t["net_amount"] - total_cost, 2)
            profit_margin_pct = round((profit / t["net_amount"] * 100.0), 2) if t["net_amount"] > 0 else 0.0

            fact = dict(t)
            fact.update({
                "product_name": p["product_name"],
                "category": p["category"],
                "subcategory": p["subcategory"],
                "brand": p["brand"],
                "unit_cost": unit_cost,
                "total_cost": total_cost,
                "profit": profit,
                "profit_margin_pct": profit_margin_pct,
                "store_name": s["store_name"],
                "store_city": s["city"],
                "store_state": s["state"],
                "region": s["region"],
                "customer_name": c["customer_name"],
                "customer_segment": c["customer_segment"],
            })
            facts.append(fact)

        self.logger.info(f"Assembled Gold Fact Sales: {len(facts):,} enriched records.")
        return facts
