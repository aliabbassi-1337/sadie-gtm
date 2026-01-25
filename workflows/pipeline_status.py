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
import sys

from loguru import logger

from db.client import init_db, close_db, get_conn
from services.leadgen.constants import PipelineStage, get_stage_label


async def get_pipeline_summary():
    """Get overall pipeline summary."""
    async with get_conn() as conn:
        results = await conn.fetch("""
            SELECT 
                status,
                COUNT(*) AS count
            FROM sadie_gtm.hotels
            GROUP BY status
            ORDER BY status DESC
        """)
        return [(r['status'], r['count']) for r in results]


async def get_pipeline_by_source():
    """Get pipeline summary grouped by source."""
    async with get_conn() as conn:
        results = await conn.fetch("""
            SELECT 
                source,
                SUM(CASE WHEN status = 0 THEN 1 ELSE 0 END) AS ingested,
                SUM(CASE WHEN status = 10 THEN 1 ELSE 0 END) AS has_website,
                SUM(CASE WHEN status = 20 THEN 1 ELSE 0 END) AS has_location,
                SUM(CASE WHEN status = 30 THEN 1 ELSE 0 END) AS detected,
                SUM(CASE WHEN status = 40 THEN 1 ELSE 0 END) AS enriched,
                SUM(CASE WHEN status = 100 THEN 1 ELSE 0 END) AS launched,
                SUM(CASE WHEN status < 0 THEN 1 ELSE 0 END) AS rejected,
                COUNT(*) AS total
            FROM sadie_gtm.hotels
            GROUP BY source
            ORDER BY total DESC
        """)
        return [dict(r) for r in results]


async def get_source_detail(source: str):
    """Get detailed status for a specific source."""
    async with get_conn() as conn:
        results = await conn.fetch("""
            SELECT 
                status,
                COUNT(*) AS count
            FROM sadie_gtm.hotels
            WHERE source = $1
            GROUP BY status
            ORDER BY status DESC
        """, source)
        return [(r['status'], r['count']) for r in results]


def print_progress_bar(value: int, total: int, width: int = 30) -> str:
    """Create a text progress bar."""
    if total == 0:
        return "░" * width
    filled = int(width * value / total)
    return "█" * filled + "░" * (width - filled)


def print_summary(summary: list):
    """Print overall pipeline summary."""
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
    
    print("\n" + "=" * 60)
    print("PIPELINE STATUS")
    print("=" * 60)
    print(f"\nTotal hotels: {total:,}")
    print(f"Launched: {launched:,} ({100*launched/total:.1f}%)" if total > 0 else "")
    
    print("\n📊 Progress Stages:")
    stage_order = ['ingested', 'has_website', 'has_location', 'detected', 'enriched', 'launched']
    for stage in stage_order:
        if stage in in_progress:
            status, count = in_progress[stage]
            bar = print_progress_bar(count, total, 20)
            print(f"  {stage:<15} {bar} {count:>6,} ({100*count/total:.1f}%)")
    
    print("\n❌ Terminal (Rejected):")
    for label, (status, count) in sorted(terminal.items(), key=lambda x: x[1][1], reverse=True):
        print(f"  {label:<20} {count:>6,}")


def print_by_source(sources: list):
    """Print pipeline by source."""
    print("\n" + "=" * 100)
    print("PIPELINE BY SOURCE")
    print("=" * 100)
    print(f"\n{'Source':<20} {'Ingested':>10} {'Website':>10} {'Location':>10} {'Detected':>10} {'Launched':>10} {'Rejected':>10} {'Total':>10}")
    print("-" * 100)
    
    for s in sources:
        print(f"{s['source'][:19]:<20} {s['ingested']:>10,} {s['has_website']:>10,} {s['has_location']:>10,} {s['detected']:>10,} {s['launched']:>10,} {s['rejected']:>10,} {s['total']:>10,}")


def print_source_detail(source: str, detail: list):
    """Print detailed status for a source."""
    print("\n" + "=" * 60)
    print(f"SOURCE: {source}")
    print("=" * 60)
    
    total = sum(count for _, count in detail)
    
    for status, count in detail:
        label = get_stage_label(status)
        bar = print_progress_bar(count, total, 30)
        pct = 100 * count / total if total > 0 else 0
        print(f"  {label:<20} {bar} {count:>6,} ({pct:.1f}%)")


async def run():
    parser = argparse.ArgumentParser(description="View pipeline status")
    parser.add_argument("--by-source", action="store_true", help="Show breakdown by source")
    parser.add_argument("--source", type=str, help="Show detail for specific source")
    
    args = parser.parse_args()
    
    logger.remove()
    await init_db()
    
    try:
        if args.source:
            detail = await get_source_detail(args.source)
            print_source_detail(args.source, detail)
        elif args.by_source:
            sources = await get_pipeline_by_source()
            print_by_source(sources)
        else:
            summary = await get_pipeline_summary()
            print_summary(summary)
    finally:
        await close_db()


if __name__ == "__main__":
    asyncio.run(run())
