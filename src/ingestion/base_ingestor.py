"""
Base Ingestor Class defining common ETL ingestion contracts and audit metadata.
"""

import os
import csv
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from src.utils.logger import get_logger


class BaseIngestor(ABC):
    """Abstract base class for all source extractors."""

    def __init__(self, name: str):
        self.name = name
        self.logger = get_logger(f"ingestion.{name}")

    @abstractmethod
    def extract(self, *args, **kwargs) -> list:
        """Extract data from the source. Must be implemented by subclasses."""
        pass

    def add_audit_metadata(self, records: list, source_identifier: str, batch_id: str = None) -> list:
        """
        Append bronze-layer lakehouse audit columns:
        - _ingested_at: UTC timestamp when pipeline read the record
        - _source_file: URI or origin identifier of the source payload
        - _batch_id: Unique batch UUID grouping the execution
        """
        if not batch_id:
            batch_id = str(uuid.uuid4())

        now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        enriched = []
        for record in records:
            item = dict(record)
            item["_ingested_at"] = now_utc
            item["_source_file"] = source_identifier
            item["_batch_id"] = batch_id
            enriched.append(item)

        return enriched

    def validate_schema(self, records: list, required_columns: set) -> tuple:
        """
        Verify that all required columns are present in extracted records.
        Returns: (is_valid: bool, missing_columns: set)
        """
        if not records:
            return True, set()

        first_row_keys = set(records[0].keys())
        missing = required_columns - first_row_keys
        if missing:
            self.logger.error(f"[{self.name}] Schema mismatch. Missing columns: {missing}")
            return False, missing
        return True, set()

    def save_to_bronze(self, records: list, output_dir: str, filename: str) -> str:
        """
        Persist ingested records into the Bronze lakehouse storage layer.
        """
        os.makedirs(output_dir, exist_ok=True)
        target_path = os.path.join(output_dir, filename)

        if not records:
            self.logger.warning(f"[{self.name}] No records to write to bronze.")
            return target_path

        fieldnames = list(records[0].keys())
        with open(target_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)

        self.logger.info(f"[{self.name}] Ingested {len(records):,} records to Bronze: {target_path}")
        return target_path
