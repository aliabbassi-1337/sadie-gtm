#!/usr/bin/env python3
"""
Pipeline Status Dashboard - View hotel processing progress.

Shows where hotels are in the pipeline and what action they need.

Usage:
    uv run python -m workflows.pipeline_status
    uv run python -m workflows.pipeline_status --by-source
    uv run python -m workflows.pipeline_status --source chicago_license
"""

import argparse
import asyncio

from loguru import logger

from db.client import init_db, close_db
from services.leadgen.constants import get_stage_label
from services.reporting.service import Service


def print_progress_bar(value: int, total: int, width: int = 30) -> str:
    """Create a text progress bar."""
    if total == 0:
        return "░" * width
    filled = int(width * value / total)
    return "█" * filled + "░" * (width - filled)


def log_summary(summary: list):
    """Log overall pipeline summary."""
    # Group by category
    in_progress = {}
    terminal = {}
    
    for status, count in summary:
        label = get_stage_label(status)
        if status >= 0:
            in_progress[label] = (status, count)
        else:
            terminal[label] = (status, count)
    
    total = sum(count for _, count in summary)
    launched = in_progress.get('launched', (100, 0))[1]
    
    logger.info("=" * 60)
    logger.info("PIPELINE STATUS")
    logger.info("=" * 60)
    logger.info(f"Total hotels: {total:,}")
    if total > 0:
        logger.info(f"Launched: {launched:,} ({100*launched/total:.1f}%)")
    
    logger.info("")
    logger.info("Progress Stages:")
    stage_order = ['ingested', 'has_website', 'has_location', 'detected', 'enriched', 'launched']
    for stage in stage_order:
        if stage in in_progress:
            status, count = in_progress[stage]
            bar = print_progress_bar(count, total, 20)
            logger.info(f"  {stage:<15} {bar} {count:>6,} ({100*count/total:.1f}%)")
    
    logger.info("")
    logger.info("Terminal (Rejected):")
    for label, (status, count) in sorted(terminal.items(), key=lambda x: x[1][1], reverse=True):
        logger.info(f"  {label:<20} {count:>6,}")


def log_by_source(sources: list):
    """Log pipeline by source."""
    logger.info("=" * 100)
    logger.info("PIPELINE BY SOURCE")
    logger.info("=" * 100)
    logger.info(f"{'Source':<20} {'Ingested':>10} {'Website':>10} {'Location':>10} {'Detected':>10} {'Launched':>10} {'Rejected':>10} {'Total':>10}")
    logger.info("-" * 100)
    
    for s in sources:
        logger.info(f"{s['source'][:19]:<20} {s['ingested']:>10,} {s['has_website']:>10,} {s['has_location']:>10,} {s['detected']:>10,} {s['launched']:>10,} {s['rejected']:>10,} {s['total']:>10,}")


def log_source_detail(source: str, detail: list):
    """Log detailed status for a source."""
    logger.info("=" * 60)
    logger.info(f"SOURCE: {source}")
    logger.info("=" * 60)
    
    total = sum(count for _, count in detail)
    
    for status, count in detail:
        label = get_stage_label(status)
        bar = print_progress_bar(count, total, 30)
        pct = 100 * count / total if total > 0 else 0
        logger.info(f"  {label:<20} {bar} {count:>6,} ({pct:.1f}%)")


async def run():
    parser = argparse.ArgumentParser(description="View pipeline status")
    parser.add_argument("--by-source", action="store_true", help="Show breakdown by source")
    parser.add_argument("--source", type=str, help="Show detail for specific source")
    
    args = parser.parse_args()
    
    await init_db()
    service = Service()
    
    try:
        if args.source:
            detail = await service.get_pipeline_by_source_name(args.source)
            log_source_detail(args.source, detail)
        elif args.by_source:
            sources = await service.get_pipeline_by_source()
            log_by_source(sources)
        else:
            summary = await service.get_pipeline_summary()
            log_summary(summary)
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(run())
