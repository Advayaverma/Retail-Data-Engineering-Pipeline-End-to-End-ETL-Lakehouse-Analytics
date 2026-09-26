"""
CSV Data Ingestor for Retail Data Engineering Pipeline.
"""

import os
import csv
from src.ingestion.base_ingestor import BaseIngestor


class CSVIngestor(BaseIngestor):
    """Ingests flat CSV data into Bronze layer with schema validation and metadata."""

    def __init__(self, source_name: str, required_columns: list):
        super().__init__(name=f"csv_{source_name}")
        self.source_name = source_name
        self.required_columns = set(required_columns)

    def extract(self, file_path: str) -> list:
        """
        Read CSV file and return raw records as list of dictionaries.
        Handles missing files and corrupted files gracefully.
        """
        if not os.path.isfile(file_path):
            self.logger.error(f"Source file not found at path: {file_path}")
            raise FileNotFoundError(f"Source file not found: {file_path}")

        records = []
        try:
            with open(file_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                if not reader.fieldnames:
                    self.logger.warning(f"File {file_path} is empty or has no header.")
                    return []

                # Validate schema
                is_valid, missing = self.validate_schema([dict.fromkeys(reader.fieldnames, "")], self.required_columns)
                if not is_valid:
                    raise ValueError(f"Schema validation failed for {file_path}. Missing columns: {missing}")

                for row in reader:
                    records.append(row)

            self.logger.info(f"Successfully extracted {len(records):,} records from {file_path}")
            return records

        except Exception as e:
            self.logger.error(f"Error reading CSV file {file_path}: {str(e)}")
            raise

    def ingest_to_bronze(self, source_file_path: str, bronze_output_dir: str, batch_id: str = None) -> tuple:
        """
        Execute full extraction, audit metadata tagging, and bronze persistence.
        Returns: (records_count: int, bronze_file_path: str)
        """
        records = self.extract(source_file_path)
        enriched_records = self.add_audit_metadata(
            records=records,
            source_identifier=os.path.basename(source_file_path),
            batch_id=batch_id
        )
        bronze_filename = f"{self.source_name}_bronze.csv"
        bronze_path = self.save_to_bronze(enriched_records, bronze_output_dir, bronze_filename)
        return len(enriched_records), bronze_path
