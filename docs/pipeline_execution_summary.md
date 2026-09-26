# Pipeline Execution Summary Report

**Run ID:** `INTEGRATION-TEST-RUN`  
**Execution Timestamp:** `2026-09-26T17:03:21.799213+00:00`  
**Overall Pipeline Status:** **SUCCESS**  
**Total Pipeline Wall Clock Time:** **23.81s**  

## Stage Breakdown

| Stage | Status | Duration (seconds) |
|---|---|---|
| Bronze Ingestion | SUCCESS | 9.47s |
| Data Quality & Quarantine | SUCCESS | 1.86s |
| Silver Transformation | SUCCESS | 4.28s |
| Incremental Delta MERGE | SUCCESS | 3.09s |
| Gold Star Schema & SCD2 | SUCCESS | 1.82s |
| Relational DW Load | SUCCESS | 1.2s |
| Analytics Reporting | SUCCESS | 2.09s |

## Architecture Lineage
```
Landing (Raw CSVs) -> Bronze (Audit Columns) -> Quality Quarantine (~1.05% Defects)
                   -> Silver (Cleansed/Typed) -> Incremental Delta Lake MERGE
                   -> Gold Star Schema (Kimball SCD2) -> Relational DW -> Analytics Report
```
