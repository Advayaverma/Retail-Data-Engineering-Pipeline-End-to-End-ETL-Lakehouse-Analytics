# Data Quality Audit Report & Quarantine Summary

- **Evaluation Date:** 2026-09-26 17:03:07
- **Dataset Evaluated:** `transactions`
- **Source Layer:** Bronze (`data/bronze/transactions/transactions_bronze.csv`)
- **Quarantine Target:** `C:\Users\Advaya\OneDrive\Desktop\project\data\quarantine\transactions_quarantine.csv`
- **Clean Stream Target:** `C:\Users\Advaya\OneDrive\Desktop\project\data\silver\staging\transactions_clean.csv`

---

## Executive Summary

| Metric | Count | Percentage |
| :--- | :--- | :--- |
| **Total Rows Ingested** | **105,210** | 100.00% |
| **Clean / Valid Rows Passed** | **104,110** | **98.95%** |
| **Quarantined Defect Rows** | **1,100** | **1.05%** |

---

## Detailed Anomaly Breakdown

| Defect Category | Error Code | Violations Detected | Remediation / Quarantine Strategy |
| :--- | :--- | :--- | :--- |
| **Duplicate Transactions** | `ERR_DUPLICATE_RECORD` | 210 | Isolated to prevent double-counting in financial facts |
| **Missing Customer IDs** | `ERR_MISSING_CUSTOMER_ID` | 253 | Routed to quarantine for guest/anonymous resolution |
| **Invalid Customer IDs** | `ERR_REF_CUSTOMER` | 220 | Fails foreign key lookup against Customer Master |
| **Invalid Unit Prices** | `ERR_INVALID_PRICE` | 155 | Negative or zero price anomaly |
| **Invalid Quantities** | `ERR_INVALID_QUANTITY` | 141 | Negative or zero items purchased |
| **Malformed Timestamps** | `ERR_MALFORMED_TIMESTAMP` | 121 | Non-parseable or corrupt date strings |
| **Other Anomalies** | Multiple | 0 | Unclassified edge cases |

---

## Key Interview Talking Points (Celebal Technologies)

1. **Why Quarantine instead of silent dropping?**
   * Dropping bad data silently leads to undetectable revenue discrepancies between accounting and data platforms.
   * By routing anomalies to `data/quarantine/` with `_defect_code` and `_quarantined_at`, upstream source teams can inspect root causes and resubmit corrected records.
2. **Medallion Gatekeeper:**
   * The Bronze layer retains raw records unconditionally.
   * The Data Quality layer acts as the gatekeeper, ensuring the Silver layer contains only pristine, trusted data.
