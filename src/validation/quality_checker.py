"""
Data Quality Checker and Quarantine Router.
Evaluates records against validation rules, isolates defects into Quarantine,
routes clean data to Silver, and generates executive audit reports.
"""

import os
import csv
from datetime import datetime
from src.utils.logger import get_logger
from src.validation.rules import (
    NotNullOrEmptyRule,
    NumericRangeRule,
    TimestampFormatRule,
    ReferentialIntegrityRule,
    DuplicateTracker
)


class DataQualityChecker:
    """Orchestrates quality evaluation and isolates non-compliant records."""

    def __init__(self, dataset_name: str = "transactions"):
        self.dataset_name = dataset_name
        self.logger = get_logger(f"quality.{dataset_name}")
        self.rules = []
        self.duplicate_tracker = None

    def add_rule(self, rule):
        """Register a validation rule."""
        self.rules.append(rule)
        return self

    def set_duplicate_tracker(self, key_field: str = "transaction_id"):
        """Enable stateful duplicate detection."""
        self.duplicate_tracker = DuplicateTracker(key_field)
        return self

    def validate_dataset(self, records: list) -> tuple:
        """
        Evaluate all records against registered rules.
        
        Returns:
            (clean_records: list, quarantine_records: list, report: dict)
        """
        self.logger.info(f"Starting Data Quality validation for {self.dataset_name} ({len(records):,} rows)...")
        clean_records = []
        quarantine_records = []

        defect_stats = {
            "duplicate_rows": 0,
            "missing_customer_ids": 0,
            "invalid_customer_ids": 0,
            "invalid_prices": 0,
            "invalid_quantities": 0,
            "invalid_timestamps": 0,
            "other_defects": 0,
        }

        now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        for record in records:
            is_valid = True
            failure_code = None
            failure_desc = None

            # 1. Duplicate Check
            if self.duplicate_tracker:
                dupe_result = self.duplicate_tracker.check_duplicate(record)
                if not dupe_result.is_valid:
                    is_valid = False
                    failure_code = dupe_result.error_code
                    failure_desc = dupe_result.error_message
                    defect_stats["duplicate_rows"] += 1

            # 2. Rule Checks (if not already failed as duplicate)
            if is_valid:
                for rule in self.rules:
                    res = rule.evaluate(record)
                    if not res.is_valid:
                        is_valid = False
                        failure_code = res.error_code
                        failure_desc = res.error_message

                        # Map to defect statistics
                        if failure_code == "ERR_MISSING_CUSTOMER_ID":
                            defect_stats["missing_customer_ids"] += 1
                        elif failure_code == "ERR_REF_CUSTOMER":
                            defect_stats["invalid_customer_ids"] += 1
                        elif "PRICE" in failure_code:
                            defect_stats["invalid_prices"] += 1
                        elif "QUANTITY" in failure_code:
                            defect_stats["invalid_quantities"] += 1
                        elif "TIMESTAMP" in failure_code:
                            defect_stats["invalid_timestamps"] += 1
                        else:
                            defect_stats["other_defects"] += 1
                        break  # Capture first primary violation

            if is_valid:
                clean_records.append(record)
            else:
                quarantine_entry = dict(record)
                quarantine_entry["_defect_code"] = failure_code
                quarantine_entry["_defect_description"] = failure_desc
                quarantine_entry["_quarantined_at"] = now_utc
                quarantine_records.append(quarantine_entry)

        # Build validation summary report
        total_rows = len(records)
        valid_rows = len(clean_records)
        quarantine_rows = len(quarantine_records)
        rejection_rate = (quarantine_rows / total_rows * 100.0) if total_rows > 0 else 0.0

        report = {
            "dataset": self.dataset_name,
            "rows_processed": total_rows,
            "valid_rows": valid_rows,
            "quarantine_rows": quarantine_rows,
            "rejection_rate_pct": round(rejection_rate, 2),
            "defect_breakdown": defect_stats,
            "timestamp": now_utc,
        }

        self.logger.info(
            f"Validation complete: {valid_rows:,} clean ({100 - rejection_rate:.2f}%), "
            f"{quarantine_rows:,} quarantined ({rejection_rate:.2f}%)"
        )
        return clean_records, quarantine_records, report

    def save_quarantine(self, quarantine_records: list, output_dir: str) -> str:
        """Persist quarantined records with defect annotations."""
        os.makedirs(output_dir, exist_ok=True)
        filename = f"{self.dataset_name}_quarantine.csv"
        target_path = os.path.join(output_dir, filename)

        if not quarantine_records:
            self.logger.info(f"No quarantined records to save for {self.dataset_name}.")
            return target_path

        fieldnames = list(quarantine_records[0].keys())
        with open(target_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(quarantine_records)

        self.logger.warning(f"Saved {len(quarantine_records):,} rejected records to Quarantine: {target_path}")
        return target_path

    @staticmethod
    def format_report_text(report: dict) -> str:
        """Format validation report into clean human-readable text."""
        stats = report["defect_breakdown"]
        lines = [
            "=" * 50,
            f"DATA QUALITY AUDIT REPORT: {report['dataset'].upper()}",
            "=" * 50,
            f"Rows processed:        {report['rows_processed']:>10,}",
            f"Valid rows:            {report['valid_rows']:>10,}",
            f"Quarantined rows:      {report['quarantine_rows']:>10,}",
            f"Rejection rate:        {report['rejection_rate_pct']:>9.2f}%",
            "-" * 50,
            "DEFECT BREAKDOWN:",
            f"  * Duplicate rows:            {stats.get('duplicate_rows', 0):>6,}",
            f"  * Missing customer IDs:      {stats.get('missing_customer_ids', 0):>6,}",
            f"  * Invalid customer IDs:      {stats.get('invalid_customer_ids', 0):>6,}",
            f"  * Invalid prices:            {stats.get('invalid_prices', 0):>6,}",
            f"  * Invalid quantities:        {stats.get('invalid_quantities', 0):>6,}",
            f"  * Invalid timestamps:        {stats.get('invalid_timestamps', 0):>6,}",
            f"  * Other anomalies:           {stats.get('other_defects', 0):>6,}",
            "=" * 50,
        ]
        return "\n".join(lines)
