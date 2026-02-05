#!/usr/bin/env python3
"""Zero to Hero: Launch, Export, and Sync in one go.

The ultimate lazy script for when you want leads without the hassle.

USAGE:
    uv run python scripts/zero_to_hero.py
    uv run python scripts/zero_to_hero.py --dry-run  # See what would happen
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import subprocess
from loguru import logger


async def run_step(name: str, cmd: list, dry_run: bool = False) -> bool:
    """Run a step and return success status."""
    logger.info(f"{'[DRY RUN] ' if dry_run else ''}Starting: {name}")
    
    if dry_run:
        logger.info(f"  Would run: {' '.join(cmd)}")
        return True
    
    try:
        result = subprocess.run(
            cmd,
            cwd=Path(__file__).parent.parent,
            capture_output=False,
        )
        if result.returncode == 0:
            logger.success(f"Completed: {name}")
            return True
        else:
            logger.error(f"Failed: {name} (exit code {result.returncode})")
            return False
    except Exception as e:
        logger.error(f"Failed: {name} - {e}")
        return False


async def main(dry_run: bool = False):
    logger.info("=" * 60)
    logger.info("ZERO TO HERO")
    logger.info("Launch -> Export -> Sync")
    logger.info("=" * 60)
    
    steps = [
        ("Launch hotels", ["uv", "run", "python", "-m", "workflows.launcher", "launch-all"]),
        ("Export all reports", ["uv", "run", "python", "-m", "workflows.export", "--all", "--no-notify"]),
        ("Sync to OneDrive", ["bash", "scripts/sync_reports.sh"]),
    ]
    
    results = []
    for name, cmd in steps:
        success = await run_step(name, cmd, dry_run)
        results.append((name, success))
        if not success and not dry_run:
            logger.warning(f"Stopping due to failure in: {name}")
            break
    
    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    for name, success in results:
        status = "OK" if success else "FAILED"
        logger.info(f"  [{status}] {name}")
    
    all_success = all(s for _, s in results)
    if all_success:
        logger.success("Zero to Hero complete!")
    else:
        logger.error("Some steps failed")
    
    return all_success


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Zero to Hero: Launch, Export, Sync")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")
    args = parser.parse_args()
    
    success = asyncio.run(main(dry_run=args.dry_run))
    sys.exit(0 if success else 1)
