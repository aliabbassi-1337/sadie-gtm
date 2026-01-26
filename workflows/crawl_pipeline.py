#!/usr/bin/env python3
"""
Crawl Data Pipeline - Full orchestration for ingesting and exporting crawl data.

This script handles the entire pipeline with checkpointing so you can resume
if something fails. Progress is tracked in a JSON file.

Usage:
    # Run full pipeline (download, ingest, export)
    uv run python -m workflows.crawl_pipeline

    # Resume from where it left off
    uv run python -m workflows.crawl_pipeline --resume

    # Check status only
    uv run python -m workflows.crawl_pipeline --status

    # Reset and start fresh
    uv run python -m workflows.crawl_pipeline --reset
"""

import argparse
import asyncio
import json
import logging
import subprocess
from datetime import datetime
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Configuration
S3_BUCKET = "sadie-gtm"
S3_PREFIX = "crawl-data/"
LOCAL_DIR = Path("data/crawl")
STATE_FILE = Path("data/crawl_pipeline_state.json")

ENGINES = {
    "cloudbeds": "cloudbeds_deduped.txt",
    "mews": "mews.txt",
    "rms": "rms.txt",
    "siteminder": "siteminder.txt",
}


def load_state() -> dict:
    """Load pipeline state from file."""
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {
        "started_at": None,
        "completed_at": None,
        "steps": {
            "download": {"status": "pending", "engines": {}},
            "ingest": {"status": "pending", "engines": {}},
            "export": {"status": "pending", "engines": {}},
        }
    }


def save_state(state: dict):
    """Save pipeline state to file."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2, default=str))


def print_status(state: dict):
    """Print current pipeline status."""
    print("\n" + "=" * 60)
    print("CRAWL PIPELINE STATUS")
    print("=" * 60)
    
    if state["started_at"]:
        print(f"Started: {state['started_at']}")
    if state["completed_at"]:
        print(f"Completed: {state['completed_at']}")
    
    print()
    
    for step_name, step in state["steps"].items():
        status_icon = {
            "pending": "⏳",
            "in_progress": "🔄",
            "completed": "✅",
            "failed": "❌",
        }.get(step["status"], "❓")
        
        print(f"{status_icon} {step_name.upper()}: {step['status']}")
        
        for engine, engine_state in step.get("engines", {}).items():
            if isinstance(engine_state, dict):
                eng_status = engine_state.get("status", "unknown")
                eng_icon = {"completed": "✅", "failed": "❌", "in_progress": "🔄"}.get(eng_status, "⏳")
                extra = ""
                if engine_state.get("count"):
                    extra = f" ({engine_state['count']} records)"
                if engine_state.get("error"):
                    extra = f" - {engine_state['error'][:50]}"
                print(f"    {eng_icon} {engine}{extra}")
            else:
                print(f"    - {engine}: {engine_state}")
    
    print("=" * 60)


async def step_download(state: dict) -> bool:
    """Step 1: Download crawl files from S3."""
    step = state["steps"]["download"]
    
    if step["status"] == "completed":
        logger.info("Download step already completed, skipping...")
        return True
    
    step["status"] = "in_progress"
    save_state(state)
    
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)
    
    all_success = True
    for engine, s3_file in ENGINES.items():
        if step["engines"].get(engine, {}).get("status") == "completed":
            logger.info(f"  {engine}: already downloaded, skipping")
            continue
        
        step["engines"][engine] = {"status": "in_progress"}
        save_state(state)
        
        s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}{s3_file}"
        local_path = LOCAL_DIR / f"{engine}.txt"
        
        logger.info(f"  Downloading {s3_path}...")
        
        try:
            result = subprocess.run(
                ["s5cmd", "cp", s3_path, str(local_path)],
                capture_output=True,
                text=True,
                timeout=120,
            )
            
            if result.returncode != 0:
                raise Exception(f"s5cmd failed: {result.stderr}")
            
            # Count lines
            line_count = len(local_path.read_text().strip().split("\n"))
            
            step["engines"][engine] = {
                "status": "completed",
                "count": line_count,
                "file": str(local_path),
            }
            logger.info(f"  {engine}: downloaded {line_count} slugs")
            
        except Exception as e:
            step["engines"][engine] = {"status": "failed", "error": str(e)}
            logger.error(f"  {engine}: FAILED - {e}")
            all_success = False
        
        save_state(state)
    
    step["status"] = "completed" if all_success else "failed"
    save_state(state)
    return all_success


async def step_ingest(state: dict) -> bool:
    """Step 2: Ingest crawl files into database."""
    from db.client import init_db
    from services.leadgen.service import Service
    
    step = state["steps"]["ingest"]
    
    if step["status"] == "completed":
        logger.info("Ingest step already completed, skipping...")
        return True
    
    # Check download step completed
    if state["steps"]["download"]["status"] != "completed":
        logger.error("Download step not completed, cannot ingest")
        return False
    
    step["status"] = "in_progress"
    save_state(state)
    
    await init_db()
    service = Service()
    
    all_success = True
    for engine in ENGINES.keys():
        if step["engines"].get(engine, {}).get("status") == "completed":
            logger.info(f"  {engine}: already ingested, skipping")
            continue
        
        step["engines"][engine] = {"status": "in_progress"}
        save_state(state)
        
        local_path = LOCAL_DIR / f"{engine}.txt"
        
        if not local_path.exists():
            step["engines"][engine] = {"status": "failed", "error": "File not found"}
            logger.error(f"  {engine}: File not found at {local_path}")
            all_success = False
            save_state(state)
            continue
        
        logger.info(f"  Ingesting {engine}...")
        
        try:
            stats = await service.ingest_crawled_urls(
                file_path=str(local_path),
                booking_engine=engine,
                source_tag="commoncrawl",
            )
            
            step["engines"][engine] = {
                "status": "completed",
                "stats": stats,
            }
            logger.info(f"  {engine}: inserted={stats.get('inserted', 0)}, updated={stats.get('updated', 0)}, errors={stats.get('errors', 0)}")
            
        except Exception as e:
            step["engines"][engine] = {"status": "failed", "error": str(e)}
            logger.error(f"  {engine}: FAILED - {e}")
            all_success = False
        
        save_state(state)
    
    step["status"] = "completed" if all_success else "failed"
    save_state(state)
    return all_success


async def step_export(state: dict) -> bool:
    """Step 3: Export crawl data to Excel files."""
    from db.client import init_db
    from services.reporting.service import Service
    
    step = state["steps"]["export"]
    
    if step["status"] == "completed":
        logger.info("Export step already completed, skipping...")
        return True
    
    # Check ingest step completed
    if state["steps"]["ingest"]["status"] != "completed":
        logger.error("Ingest step not completed, cannot export")
        return False
    
    step["status"] = "in_progress"
    save_state(state)
    
    await init_db()
    service = Service()
    
    all_success = True
    for engine in ENGINES.keys():
        if step["engines"].get(engine, {}).get("status") == "completed":
            logger.info(f"  {engine}: already exported, skipping")
            continue
        
        step["engines"][engine] = {"status": "in_progress"}
        save_state(state)
        
        logger.info(f"  Exporting {engine}...")
        
        try:
            s3_uri, count = await service.export_by_booking_engine(
                booking_engine=engine,
                source_pattern="%commoncrawl%",
            )
            
            step["engines"][engine] = {
                "status": "completed",
                "s3_uri": s3_uri,
                "count": count,
            }
            logger.info(f"  {engine}: exported {count} leads to {s3_uri}")
            
        except Exception as e:
            step["engines"][engine] = {"status": "failed", "error": str(e)}
            logger.error(f"  {engine}: FAILED - {e}")
            all_success = False
        
        save_state(state)
    
    step["status"] = "completed" if all_success else "failed"
    save_state(state)
    return all_success


async def run_pipeline(resume: bool = False):
    """Run the full pipeline."""
    state = load_state()
    
    if not resume and state["started_at"]:
        logger.warning("Pipeline already started. Use --resume to continue or --reset to start fresh.")
        print_status(state)
        return
    
    if not state["started_at"]:
        state["started_at"] = datetime.now().isoformat()
        save_state(state)
    
    logger.info("=" * 60)
    logger.info("CRAWL DATA PIPELINE")
    logger.info("=" * 60)
    
    # Step 1: Download
    logger.info("\n[STEP 1/3] Downloading crawl files from S3...")
    if not await step_download(state):
        logger.error("Download step failed. Fix issues and run with --resume")
        print_status(state)
        return
    
    # Step 2: Ingest
    logger.info("\n[STEP 2/3] Ingesting crawl data into database...")
    if not await step_ingest(state):
        logger.error("Ingest step failed. Fix issues and run with --resume")
        print_status(state)
        return
    
    # Step 3: Export
    logger.info("\n[STEP 3/3] Exporting crawl data to Excel...")
    if not await step_export(state):
        logger.error("Export step failed. Fix issues and run with --resume")
        print_status(state)
        return
    
    state["completed_at"] = datetime.now().isoformat()
    save_state(state)
    
    logger.info("\n" + "=" * 60)
    logger.info("PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info("=" * 60)
    print_status(state)


def main():
    parser = argparse.ArgumentParser(
        description="Crawl Data Pipeline - Full orchestration",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Run full pipeline
    uv run python -m workflows.crawl_pipeline

    # Resume from where it left off
    uv run python -m workflows.crawl_pipeline --resume

    # Check status
    uv run python -m workflows.crawl_pipeline --status

    # Reset and start fresh
    uv run python -m workflows.crawl_pipeline --reset
        """
    )
    
    parser.add_argument(
        "--resume", "-r",
        action="store_true",
        help="Resume from last checkpoint"
    )
    parser.add_argument(
        "--status", "-s",
        action="store_true",
        help="Show current status only"
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset state and start fresh"
    )
    
    args = parser.parse_args()
    
    if args.reset:
        if STATE_FILE.exists():
            STATE_FILE.unlink()
            logger.info("State reset. Ready to start fresh.")
        else:
            logger.info("No state file found.")
        return
    
    if args.status:
        state = load_state()
        print_status(state)
        return
    
    asyncio.run(run_pipeline(resume=args.resume))


if __name__ == "__main__":
    main()
