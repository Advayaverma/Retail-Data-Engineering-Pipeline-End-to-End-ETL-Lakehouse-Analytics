# Pipeline Execution Summary Report

**Run ID:** `RUN-20260926165126`  
**Execution Timestamp:** `2026-09-26T16:51:55.650213+00:00`  
**Overall Pipeline Status:** **SUCCESS**  
**Total Pipeline Wall Clock Time:** **28.70s**  

## Stage Breakdown

| Stage | Status | Duration (seconds) |
|---|---|---|
| Bronze Ingestion | SUCCESS | 9.47s |
| Data Quality & Quarantine | SUCCESS | 2.49s |
| Silver Transformation | SUCCESS | 5.65s |
| Incremental Delta MERGE | SUCCESS | 4.42s |
| Gold Star Schema & SCD2 | SUCCESS | 2.51s |
| Relational DW Load | SUCCESS | 1.62s |
| Analytics Reporting | SUCCESS | 2.53s |

## Architecture Lineage
```
Landing (Raw CSVs) -> Bronze (Audit Columns) -> Quality Quarantine (~1.05% Defects)
                   -> Silver (Cleansed/Typed) -> Incremental Delta Lake MERGE
                   -> Gold Star Schema (Kimball SCD2) -> Relational DW -> Analytics Report
```
