# Pipeline Execution Summary Report

**Run ID:** `INTEGRATION-TEST-RUN`  
**Execution Timestamp:** `2026-09-26T18:09:44.314553+00:00`  
**Overall Pipeline Status:** **SUCCESS**  
**Total Pipeline Wall Clock Time:** **26.72s**  

## Stage Breakdown

| Stage | Status | Duration (seconds) |
|---|---|---|
| Bronze Ingestion | SUCCESS | 9.93s |
| Data Quality & Quarantine | SUCCESS | 2.59s |
| Silver Transformation | SUCCESS | 5.9s |
| Incremental Delta MERGE | SUCCESS | 3.21s |
| Gold Star Schema & SCD2 | SUCCESS | 1.78s |
| Relational DW Load | SUCCESS | 1.19s |
| Analytics Reporting | SUCCESS | 2.12s |

## Architecture Lineage
```
Landing (Raw CSVs) -> Bronze (Audit Columns) -> Quality Quarantine (~1.05% Defects)
                   -> Silver (Cleansed/Typed) -> Incremental Delta Lake MERGE
                   -> Gold Star Schema (Kimball SCD2) -> Relational DW -> Analytics Report
```
