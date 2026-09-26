"""
REST API Ingestor for Currency Exchange Rates.
Implements exponential backoff, retry handling, timeouts, and graceful fallbacks.
"""

import os
import json
import time
import urllib.request
import urllib.error
from src.ingestion.base_ingestor import BaseIngestor

FALLBACK_RATES = {
    "base": "USD",
    "date": "2026-01-01",
    "rates": {
        "EUR": 0.924,
        "GBP": 0.789,
        "INR": 83.480,
        "CAD": 1.365,
        "AUD": 1.512,
        "JPY": 155.250,
    }
}


class APIIngestor(BaseIngestor):
    """Ingests foreign currency exchange rates via REST API."""

    def __init__(self, api_url: str, base_currency: str = "USD", max_retries: int = 3, timeout_seconds: int = 5):
        super().__init__(name="api_exchange_rates")
        self.api_url = api_url
        self.base_currency = base_currency
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds

    def extract(self, simulate_failure: bool = False) -> dict:
        """
        Fetch exchange rates from REST API with exponential backoff.
        Falls back to local baseline rates if API is unavailable.
        """
        query_url = f"{self.api_url}?base={self.base_currency}"
        if simulate_failure:
            query_url += "&simulate_error=true"

        for attempt in range(1, self.max_retries + 1):
            try:
                self.logger.info(f"Connecting to REST API (Attempt {attempt}/{self.max_retries}): {query_url}")
                req = urllib.request.Request(
                    query_url,
                    headers={"User-Agent": "RetailDataPipeline/1.0", "Accept": "application/json"}
                )
                with urllib.request.urlopen(req, timeout=self.timeout_seconds) as response:
                    if response.status == 200:
                        payload = json.loads(response.read().decode("utf-8"))
                        self.logger.info(f"Successfully received {len(payload.get('rates', {}))} exchange rates from API.")
                        return payload
                    else:
                        self.logger.warning(f"Unexpected API response code: {response.status}")

            except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, ConnectionRefusedError) as e:
                self.logger.warning(f"API attempt {attempt} failed: {str(e)}")
                if attempt < self.max_retries:
                    sleep_time = 0.5 * (2 ** (attempt - 1))
                    time.sleep(sleep_time)

        self.logger.warning("All API retry attempts exhausted. Engaging offline fallback exchange rates.")
        return FALLBACK_RATES

    def transform_payload_to_rows(self, payload: dict) -> list:
        """
        Convert hierarchical API JSON payload to flat relational rows.
        """
        base = payload.get("base", self.base_currency)
        rate_date = payload.get("date", "2026-01-01")
        rates = payload.get("rates", {})

        rows = []
        # Include base rate 1.0 for USD
        rows.append({
            "base_currency": base,
            "target_currency": base,
            "exchange_rate": 1.0,
            "effective_date": rate_date,
        })

        for target_curr, rate in rates.items():
            rows.append({
                "base_currency": base,
                "target_currency": target_curr,
                "exchange_rate": float(rate),
                "effective_date": rate_date,
            })
        return rows

    def ingest_to_bronze(self, bronze_output_dir: str, batch_id: str = None) -> tuple:
        """
        Extract API rates, flatten, tag audit columns, and write to Bronze layer.
        """
        payload = self.extract()
        rows = self.transform_payload_to_rows(payload)
        enriched = self.add_audit_metadata(
            records=rows,
            source_identifier=self.api_url,
            batch_id=batch_id
        )
        bronze_path = self.save_to_bronze(enriched, bronze_output_dir, "exchange_rates_bronze.csv")
        return len(enriched), bronze_path
