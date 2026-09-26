"""
Slowly Changing Dimension Type 2 (SCD Type 2) Handler.

Tracks historical changes to customer dimensions over time:
- Generates incrementing surrogate keys (customer_sk)
- Preserves full audit history by expiring superseded records (is_current=False)
- Inserts new active versions (is_current=True, effective_end_date='9999-12-31')
- Enables point-in-time fact joins (transaction_date BETWEEN start_date AND end_date)

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
from datetime import datetime
from src.utils.logger import get_logger

MAX_FUTURE_DATE = "9999-12-31"


class SCD2CustomerHandler:
    """Manages Slowly Changing Dimension Type 2 lifecycle for customers."""

    def __init__(self, tracked_attributes: list = None):
        self.tracked_attributes = tracked_attributes or ["customer_segment", "city", "state"]
        self.logger = get_logger("modelling.scd2")

    def initialize_dim_customer(self, baseline_customers: list) -> list:
        """
        Build initial SCD Type 2 dimension from baseline customer records.
        All records start as active (is_current=True).
        """
        dim_rows = []
        for idx, cust in enumerate(baseline_customers, start=1):
            signup_dt = cust.get("signup_date", "2021-01-01")
            dim_rows.append({
                "customer_sk": idx,
                "customer_id": cust["customer_id"],
                "customer_name": cust.get("customer_name", ""),
                "email": cust.get("email", ""),
                "city": cust.get("city", ""),
                "state": cust.get("state", ""),
                "customer_segment": cust.get("customer_segment", "Standard"),
                "effective_start_date": signup_dt,
                "effective_end_date": MAX_FUTURE_DATE,
                "is_current": True,
            })

        self.logger.info(f"Initialized dim_customer with {len(dim_rows):,} active records (Version 1).")
        return dim_rows

    def process_scd2_updates(
        self,
        current_dim_customer: list,
        incoming_updates: list,
        update_effective_date: str = "2026-01-01"
    ) -> tuple:
        """
        Apply SCD Type 2 updates:
        - If tracked attributes changed: Expire existing record and insert new active record.
        - If new customer: Insert with is_current=True.
        
        Returns:
            (updated_dim_customer: list, num_expired: int, num_inserted: int)
        """
        self.logger.info(f"Processing SCD Type 2 updates for {len(incoming_updates):,} incoming customer records...")

        # Map current active records by natural customer_id
        active_map = {}
        for r in current_dim_customer:
            if r["is_current"]:
                active_map[r["customer_id"]] = r

        max_sk = max(r["customer_sk"] for r in current_dim_customer) if current_dim_customer else 0
        expired_count = 0
        new_versions_count = 0
        brand_new_count = 0

        new_dim_records = list(current_dim_customer)

        for incoming in incoming_updates:
            cid = incoming["customer_id"]
            effective_date = incoming.get("effective_update_date", update_effective_date)

            if cid in active_map:
                existing = active_map[cid]
                # Check if any tracked attribute changed
                has_changed = any(
                    str(incoming.get(attr, "")).strip().lower() != str(existing.get(attr, "")).strip().lower()
                    for attr in self.tracked_attributes
                )

                if has_changed:
                    # 1. Expire existing record
                    existing["effective_end_date"] = effective_date
                    existing["is_current"] = False
                    expired_count += 1

                    # 2. Insert new active version with incremented surrogate key
                    max_sk += 1
                    new_version = {
                        "customer_sk": max_sk,
                        "customer_id": cid,
                        "customer_name": incoming.get("customer_name", existing["customer_name"]),
                        "email": incoming.get("email", existing["email"]),
                        "city": incoming.get("city", existing["city"]),
                        "state": incoming.get("state", existing["state"]),
                        "customer_segment": incoming.get("customer_segment", existing["customer_segment"]),
                        "effective_start_date": effective_date,
                        "effective_end_date": MAX_FUTURE_DATE,
                        "is_current": True,
                    }
                    new_dim_records.append(new_version)
                    active_map[cid] = new_version  # Update active reference
                    new_versions_count += 1
            else:
                # Brand new customer inserted directly
                max_sk += 1
                brand_new_record = {
                    "customer_sk": max_sk,
                    "customer_id": cid,
                    "customer_name": incoming.get("customer_name", ""),
                    "email": incoming.get("email", ""),
                    "city": incoming.get("city", ""),
                    "state": incoming.get("state", ""),
                    "customer_segment": incoming.get("customer_segment", "Standard"),
                    "effective_start_date": effective_date,
                    "effective_end_date": MAX_FUTURE_DATE,
                    "is_current": True,
                }
                new_dim_records.append(brand_new_record)
                active_map[cid] = brand_new_record
                brand_new_count += 1

        self.logger.info(
            f"SCD Type 2 processing complete: {expired_count:,} historical records expired, "
            f"{new_versions_count:,} new versions inserted, {brand_new_count:,} new customers added. "
            f"Total dimension records: {len(new_dim_records):,}."
        )
        return new_dim_records, expired_count, new_versions_count

    def resolve_customer_sk(self, dim_customer: list, customer_id: str, transaction_date: str) -> int:
        """
        Point-in-Time Surrogate Key Resolver:
        Finds the exact customer_sk valid as of the transaction date.
        """
        for r in dim_customer:
            if r["customer_id"] == customer_id:
                if r["effective_start_date"] <= transaction_date <= r["effective_end_date"]:
                    return r["customer_sk"]
        # Fallback to current active record if dates edge past bounds
        for r in dim_customer:
            if r["customer_id"] == customer_id and r["is_current"]:
                return r["customer_sk"]
        return -1
