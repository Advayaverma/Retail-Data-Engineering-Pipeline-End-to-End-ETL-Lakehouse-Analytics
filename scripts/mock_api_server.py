#!/usr/bin/env python3
"""
Lightweight REST API Server for Currency Exchange Rates.
Uses standard library http.server to provide a realistic, zero-dependency endpoint:
- Endpoint: GET /api/v1/rates?base=USD
- Health:   GET /health
- Failure testing: GET /api/v1/rates?simulate_error=true
"""

import sys
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from datetime import datetime

DEFAULT_PORT = 8000

EXCHANGE_RATES = {
    "USD": {
        "EUR": 0.924,
        "GBP": 0.789,
        "INR": 83.480,
        "CAD": 1.365,
        "AUD": 1.512,
        "JPY": 155.250,
    }
}


class ExchangeRateAPIHandler(BaseHTTPRequestHandler):
    """Handles REST API requests for exchange rates."""

    def _send_json_response(self, status_code: int, data: dict):
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data, indent=2).encode("utf-8"))

    def do_GET(self):
        parsed_url = urlparse(self.path)
        path = parsed_url.path
        query_params = parse_qs(parsed_url.query)

        # Health check endpoint
        if path == "/health":
            self._send_json_response(200, {
                "status": "UP",
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "service": "Retail Exchange Rate Service"
            })
            return

        # Exchange rate endpoint
        if path == "/api/v1/rates":
            # Simulate failure if requested
            if "simulate_error" in query_params:
                self._send_json_response(500, {
                    "error": "InternalServerError",
                    "message": "Simulated upstream banking service timeout"
                })
                return

            base_currency = query_params.get("base", ["USD"])[0].upper()
            rates = EXCHANGE_RATES.get(base_currency)

            if not rates:
                self._send_json_response(400, {
                    "error": "InvalidBaseCurrency",
                    "message": f"Currency '{base_currency}' not supported. Supported bases: {list(EXCHANGE_RATES.keys())}"
                })
                return

            response_payload = {
                "base": base_currency,
                "date": datetime.utcnow().strftime("%Y-%m-%d"),
                "timestamp": int(datetime.utcnow().timestamp()),
                "rates": rates
            }
            self._send_json_response(200, response_payload)
            return

        # Route not found
        self._send_json_response(404, {
            "error": "NotFound",
            "message": f"Path '{path}' not found on this API"
        })

    def log_message(self, format, *args):
        # Suppress verbose default http.server logging to keep terminal clean
        return


def run_server(port: int = DEFAULT_PORT):
    server_address = ("127.0.0.1", port)
    httpd = HTTPServer(server_address, ExchangeRateAPIHandler)
    print(f"[*] Mock Exchange Rate API Server listening on http://127.0.0.1:{port}/api/v1/rates")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[!] Shutting down API server...")
        httpd.server_close()


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_PORT
    run_server(port)
