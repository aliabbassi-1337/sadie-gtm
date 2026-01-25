#!/usr/bin/env python3
"""
Distributed Cloudbeds Scraper - Split workload across multiple EC2 instances.

Each EC2 instance has a different public IP, avoiding rate limits.

Usage:
    # On each EC2 instance, run with different worker ID:
    uv run python scripts/scrapers/cloudbeds_distributed.py --worker 0 --total-workers 5
    uv run python scripts/scrapers/cloudbeds_distributed.py --worker 1 --total-workers 5
    # etc...

    # Or run all workers via SSH from local machine:
    uv run python scripts/scrapers/cloudbeds_distributed.py --orchestrate --hosts hosts.txt

Example hosts.txt:
    ubuntu@10.0.1.1
    ubuntu@10.0.1.2
    ubuntu@10.0.1.3
"""

import argparse
import asyncio
import json
import subprocess
import sys
from pathlib import Path
from typing import List

from loguru import logger


def get_worker_slice(total_items: int, worker_id: int, total_workers: int) -> tuple:
    """Calculate start and end indices for a worker."""
    items_per_worker = total_items // total_workers
    remainder = total_items % total_workers
    
    start = worker_id * items_per_worker + min(worker_id, remainder)
    end = start + items_per_worker + (1 if worker_id < remainder else 0)
    
    return start, end


async def run_worker(worker_id: int, total_workers: int, output_dir: Path) -> None:
    """Run scraper for this worker's slice."""
    # Import here to avoid loading everything just for orchestration
    from scripts.scrapers.cloudbeds_hotels import CloudbedsGroupScraper, Hotel
    
    # Load groups
    groups_file = Path(__file__).parent.parent.parent / "data" / "cloudbeds_sitemap_leads.json"
    with open(groups_file) as f:
        groups = json.load(f)
    
    subdomains = [g['subdomain'] for g in groups if not g.get('is_demo', False)]
    total = len(subdomains)
    
    # Get this worker's slice
    start, end = get_worker_slice(total, worker_id, total_workers)
    worker_subdomains = subdomains[start:end]
    
    logger.info(f"Worker {worker_id}/{total_workers}: Processing {len(worker_subdomains)} groups ({start}-{end})")
    
    # Run scraper
    async with CloudbedsGroupScraper(concurrency=3, delay=2.0) as scraper:
        hotels = await scraper.scrape_all(worker_subdomains)
    
    # Save results
    output_file = output_dir / f"cloudbeds_hotels_worker_{worker_id}.json"
    with open(output_file, 'w') as f:
        json.dump([{
            'name': h.name,
            'city': h.city,
            'slug': h.slug,
            'property_id': h.property_id,
            'booking_url': h.booking_url,
            'group_subdomain': h.group_subdomain,
        } for h in hotels], f, indent=2)
    
    logger.info(f"Worker {worker_id}: Saved {len(hotels)} hotels to {output_file}")


def orchestrate(hosts_file: str, key: str = None) -> None:
    """Launch workers on multiple EC2 instances via SSH."""
    with open(hosts_file) as f:
        hosts = [line.strip() for line in f if line.strip() and not line.startswith('#')]
    
    if not hosts:
        logger.error("No hosts found in hosts file")
        sys.exit(1)
    
    logger.info(f"Launching {len(hosts)} workers on EC2 instances...")
    
    processes = []
    for i, host in enumerate(hosts):
        cmd = f"cd ~/sadie-gtm && ~/.local/bin/uv run python scripts/scrapers/cloudbeds_distributed.py --worker {i} --total-workers {len(hosts)}"
        
        ssh_cmd = ["ssh", "-o", "StrictHostKeyChecking=no"]
        if key:
            ssh_cmd.extend(["-i", key])
        ssh_cmd.extend([host, cmd])
        
        logger.info(f"  Starting worker {i} on {host}")
        proc = subprocess.Popen(ssh_cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        processes.append((host, proc))
    
    # Wait for all to complete
    logger.info("Waiting for workers to complete...")
    for host, proc in processes:
        stdout, _ = proc.communicate()
        logger.info(f"Worker on {host} finished (exit code: {proc.returncode})")
        if proc.returncode != 0:
            logger.error(f"  Output: {stdout.decode()[-500:]}")
    
    logger.info("All workers complete. Merge results with:")
    logger.info("  uv run python scripts/scrapers/cloudbeds_distributed.py --merge")


def merge_results(output_dir: Path) -> None:
    """Merge worker results into single file."""
    all_hotels = []
    
    for f in output_dir.glob("cloudbeds_hotels_worker_*.json"):
        with open(f) as fp:
            hotels = json.load(fp)
            all_hotels.extend(hotels)
            logger.info(f"Loaded {len(hotels)} from {f.name}")
    
    # Dedupe
    seen = set()
    unique = []
    for h in all_hotels:
        key = (h['name'], h['slug'] or h.get('property_id'))
        if key not in seen:
            seen.add(key)
            unique.append(h)
    
    output_file = output_dir / "cloudbeds_hotels_merged.json"
    with open(output_file, 'w') as f:
        json.dump(unique, f, indent=2)
    
    logger.info(f"Merged {len(unique)} unique hotels to {output_file}")


def main():
    parser = argparse.ArgumentParser(description="Distributed Cloudbeds scraper")
    parser.add_argument("--worker", type=int, help="Worker ID (0-indexed)")
    parser.add_argument("--total-workers", type=int, help="Total number of workers")
    parser.add_argument("--output-dir", type=str, default="data", help="Output directory")
    parser.add_argument("--orchestrate", action="store_true", help="Launch workers on EC2 instances")
    parser.add_argument("--hosts", type=str, help="File with EC2 hosts (one per line)")
    parser.add_argument("--key", "-i", type=str, help="SSH key file")
    parser.add_argument("--merge", action="store_true", help="Merge worker results")
    args = parser.parse_args()
    
    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")
    
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True)
    
    if args.orchestrate:
        if not args.hosts:
            logger.error("--hosts required with --orchestrate")
            sys.exit(1)
        orchestrate(args.hosts, args.key)
    elif args.merge:
        merge_results(output_dir)
    elif args.worker is not None and args.total_workers:
        asyncio.run(run_worker(args.worker, args.total_workers, output_dir))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
