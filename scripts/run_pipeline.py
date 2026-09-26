#!/usr/bin/env python3
"""
Master Pipeline Orchestration Runner (Phase 11).

Executes the entire end-to-end Retail Lakehouse Data Pipeline:
1. Ingestion: Landed CSVs -> Bronze with audit lineage metadata
2. Data Quality & Quarantine: Strict validation & defect isolation
3. Silver Transformation: Conformed cleansing, standardization & derived fields
4. Incremental Delta MERGE: ACID transactions, schema evolution & Time Travel
5. Gold Star Schema: Kimball dimensional modeling & SCD Type 2 customer history
6. Relational DW Load: Loading dimensions & facts into analytical warehouse
7. Business Analytics Reporting: 10 core analytical queries & executive KPIs

Features:
- Step-by-step DAG stage execution with timing & record count metrics
- Resilient error handling, logging, and execution summary report
- Configurable stage execution (full or selective stages)

Author: Retail Data Engineering Project
Target: Celebal Technologies Data Engineer Evaluation
"""

import os
import sys
import time
import argparse
from datetime import datetime, timezone

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.utils.logger import get_logger

# Import pipeline components
from scripts.run_ingestion import run_all_ingestions
from scripts.run_data_quality import run_data_quality_pipeline
from scripts.run_transformations import run_all_transformations
from scripts.run_incremental import run_incremental_pipeline
from scripts.build_star_schema import run_build_star_schema
from scripts.load_gold_dw import load_gold_star_schema
from scripts.run_analytics_report import execute_queries


class MasterPipelineOrchestrator:
    """End-to-End Retail Data Lakehouse Pipeline Orchestrator."""

    def __init__(self, run_id: str = None):
        self.logger = get_logger("pipeline.orchestrator")
        self.run_id = run_id or f"RUN-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        self.stage_results = {}
        self.start_time = None
        self.end_time = None

    def execute_stage(self, stage_num: int, stage_name: str, stage_fn, *args, **kwargs):
        """Execute an individual pipeline stage with robust timing, logging, and error handling."""
        self.logger.info("=" * 80)
        self.logger.info(f"STAGE [{stage_num}/7]: {stage_name.upper()} [Run ID: {self.run_id}]")
        self.logger.info("=" * 80)

        stage_start = time.time()
        try:
            result = stage_fn(*args, **kwargs)
            duration = time.time() - stage_start
            self.stage_results[stage_name] = {
                "status": "SUCCESS",
                "duration_seconds": round(duration, 2),
                "details": result
            }
            self.logger.info(f"--> STAGE [{stage_name}] COMPLETED SUCCESSFULLY in {duration:.2f}s.\n")
            return result
        except Exception as e:
            duration = time.time() - stage_start
            self.logger.error(f"--> STAGE [{stage_name}] FAILED after {duration:.2f}s: {str(e)}", exc_info=True)
            self.stage_results[stage_name] = {
                "status": "FAILED",
                "duration_seconds": round(duration, 2),
                "error": str(e)
            }
            raise

    def run_full_pipeline(self) -> dict:
        """Run all 7 stages of the lakehouse pipeline sequentially."""
        self.start_time = time.time()
        self.logger.info("#" * 80)
        self.logger.info(f"STARTING FULL END-TO-END RETAIL LAKEHOUSE PIPELINE")
        self.logger.info(f"Run ID: {self.run_id} | UTC: {datetime.now(timezone.utc).isoformat()}")
        self.logger.info("#" * 80)

        try:
            # Stage 1: Ingestion to Bronze
            ingestion_res = self.execute_stage(
                1, "Bronze Ingestion",
                run_all_ingestions, batch_id=self.run_id
            )

            # Stage 2: Data Quality & Quarantine
            dq_res = self.execute_stage(
                2, "Data Quality & Quarantine",
                run_data_quality_pipeline
            )

            # Stage 3: Silver Transformation & Cleansing
            transform_res = self.execute_stage(
                3, "Silver Transformation",
                run_all_transformations
            )

            # Stage 4: Incremental Delta Lake MERGE & Time Travel
            incremental_res = self.execute_stage(
                4, "Incremental Delta MERGE",
                run_incremental_pipeline
            )

            # Stage 5: Gold Star Schema & SCD Type 2
            star_schema_res = self.execute_stage(
                5, "Gold Star Schema & SCD2",
                run_build_star_schema
            )

            # Stage 6: Relational Warehouse Load
            dw_load_res = self.execute_stage(
                6, "Relational DW Load",
                load_gold_star_schema
            )

            # Stage 7: Business Analytics Reporting
            analytics_res = self.execute_stage(
                7, "Analytics Reporting",
                execute_queries
            )

            overall_status = "SUCCESS"

        except Exception as e:
            overall_status = "FAILED"
            self.logger.error(f"Master pipeline run terminated prematurely due to failure: {e}")

        self.end_time = time.time()
        total_duration = self.end_time - self.start_time

        summary = self.generate_summary_report(overall_status, total_duration)
        return summary

    def generate_summary_report(self, overall_status: str, total_duration: float) -> dict:
        """Generate formatted executive summary report."""
        summary = {
            "run_id": self.run_id,
            "overall_status": overall_status,
            "total_duration_seconds": round(total_duration, 2),
            "stages": self.stage_results,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

        self.logger.info("\n" + "=" * 80)
        self.logger.info(f"MASTER PIPELINE EXECUTION SUMMARY REPORT — STATUS: {overall_status}")
        self.logger.info("=" * 80)
        self.logger.info(f"Total Execution Time: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
        self.logger.info("-" * 80)
        self.logger.info(f"{'Stage Name':<32} | {'Status':<10} | {'Duration (s)':<12}")
        self.logger.info("-" * 80)

        for name, meta in self.stage_results.items():
            self.logger.info(f"{name:<32} | {meta['status']:<10} | {meta['duration_seconds']:<12.2f}")

        self.logger.info("=" * 80)

        # Write summary report artifact to docs/
        summary_path = os.path.join(PROJECT_ROOT, "docs", "pipeline_execution_summary.md")
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write(f"# Pipeline Execution Summary Report\n\n")
            f.write(f"**Run ID:** `{self.run_id}`  \n")
            f.write(f"**Execution Timestamp:** `{summary['timestamp']}`  \n")
            f.write(f"**Overall Pipeline Status:** **{overall_status}**  \n")
            f.write(f"**Total Pipeline Wall Clock Time:** **{total_duration:.2f}s**  \n\n")
            f.write(f"## Stage Breakdown\n\n")
            f.write(f"| Stage | Status | Duration (seconds) |\n")
            f.write(f"|---|---|---|\n")
            for name, meta in self.stage_results.items():
                f.write(f"| {name} | {meta['status']} | {meta['duration_seconds']}s |\n")
            f.write(f"\n## Architecture Lineage\n")
            f.write("```\n")
            f.write("Landing (Raw CSVs) -> Bronze (Audit Columns) -> Quality Quarantine (~1.05% Defects)\n")
            f.write("                   -> Silver (Cleansed/Typed) -> Incremental Delta Lake MERGE\n")
            f.write("                   -> Gold Star Schema (Kimball SCD2) -> Relational DW -> Analytics Report\n")
            f.write("```\n")

        self.logger.info(f"Pipeline summary markdown saved to: {summary_path}")
        return summary


def main():
    parser = argparse.ArgumentParser(description="Master Pipeline Orchestration Runner")
    parser.add_argument("--run-id", type=str, default=None, help="Custom Run ID for pipeline audit")
    args = parser.parse_args()

    orchestrator = MasterPipelineOrchestrator(run_id=args.run_id)
    summary = orchestrator.run_full_pipeline()

    if summary["overall_status"] != "SUCCESS":
        sys.exit(1)


if __name__ == "__main__":
    main()
