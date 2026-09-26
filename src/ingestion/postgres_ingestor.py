"""
PostgreSQL Data Ingestor for Customer Master tables.
Handles database connectivity, query extraction, connection failure recovery, and offline fallback.
"""

import os
import csv
from src.ingestion.base_ingestor import BaseIngestor


class PostgresIngestor(BaseIngestor):
    """Extracts customer master data from PostgreSQL, with resilient file fallback."""

    def __init__(
        self,
        db_config: dict,
        query: str = "SELECT * FROM retail_source.customers",
        fallback_csv_path: str = None
    ):
        super().__init__(name="postgres_customers")
        self.db_config = db_config
        self.query = query
        self.fallback_csv_path = fallback_csv_path

    def _extract_from_db(self) -> list:
        """Attempt extraction using psycopg2 / SQLAlchemy."""
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor

            conn = psycopg2.connect(
                host=self.db_config.get("host", "localhost"),
                port=self.db_config.get("port", 5432),
                database=self.db_config.get("name", "retail_dw"),
                user=self.db_config.get("user", "retail_admin"),
                password=self.db_config.get("password", "retail_secure_pass_2027"),
                connect_timeout=3
            )
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            cursor.execute(self.query)
            rows = [dict(r) for r in cursor.fetchall()]
            cursor.close()
            conn.close()
            self.logger.info(f"Extracted {len(rows):,} records directly from PostgreSQL query.")
            return rows

        except Exception as e:
            self.logger.warning(f"PostgreSQL connection failed: {str(e)}")
            raise ConnectionError(f"Unable to connect to PostgreSQL: {e}")

    def _extract_from_fallback(self) -> list:
        """Fallback to local CSV if PostgreSQL is not yet initialized or reachable."""
        if not self.fallback_csv_path or not os.path.isfile(self.fallback_csv_path):
            raise FileNotFoundError(f"Neither PostgreSQL nor fallback file {self.fallback_csv_path} is available.")

        self.logger.info(f"Engaging fallback source for customers: {self.fallback_csv_path}")
        with open(self.fallback_csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            return list(reader)

    def extract(self) -> list:
        """
        Attempt database extraction first. If connection fails, seamlessly use fallback file.
        """
        try:
            return self._extract_from_db()
        except Exception:
            self.logger.warning("PostgreSQL unreachable. Safely falling back to customer baseline file.")
            return self._extract_from_fallback()

    def ingest_to_bronze(self, bronze_output_dir: str, batch_id: str = None) -> tuple:
        """
        Execute customer extraction, tag audit metadata, and write to Bronze layer.
        """
        records = self.extract()
        source_id = "postgres://retail_source.customers" if records else "fallback_customers.csv"
        enriched = self.add_audit_metadata(
            records=records,
            source_identifier=source_id,
            batch_id=batch_id
        )
        bronze_path = self.save_to_bronze(enriched, bronze_output_dir, "customers_bronze.csv")
        return len(enriched), bronze_path
