#!/usr/bin/env python3
"""
Discover IPMS247 slugs from Wayback Machine and Common Crawl.

Usage:
    python -m workflows.discover_ipms247_slugs
    python -m workflows.discover_ipms247_slugs --output ipms247_slugs.txt
    python -m workflows.discover_ipms247_slugs --s3-upload
"""

import argparse
import asyncio
import json
import re
import os
from typing import Set, Optional

import httpx
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


WAYBACK_CDX_URL = "https://web.archive.org/cdx/search/cdx"
CC_INDEX_URL = "https://index.commoncrawl.org"
def get_brightdata_dc_proxy() -> Optional[str]:
    """Get Brightdata datacenter proxy URL for Common Crawl."""
    customer_id = os.getenv("BRIGHTDATA_CUSTOMER_ID", "")
    dc_zone = os.getenv("BRIGHTDATA_DC_ZONE", "")
    dc_password = os.getenv("BRIGHTDATA_DC_PASSWORD", "")
    if customer_id and dc_zone and dc_password:
        return f"http://brd-customer-{customer_id}-zone-{dc_zone}:{dc_password}@brd.superproxy.io:22225"
    return None


async def fetch_wayback_urls(client: httpx.AsyncClient) -> Set[str]:
    """Fetch IPMS247 URLs from Wayback Machine CDX API."""
    logger.info("Fetching from Wayback Machine...")
    urls = set()
    
    params = {
        "url": "live.ipms247.com/booking/book-rooms-*",
        "output": "json",
        "limit": 100000,
        "fl": "original",
        "collapse": "urlkey",
        "filter": "statuscode:200",
    }
    
    try:
        resp = await client.get(WAYBACK_CDX_URL, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        
        for row in data[1:]:  # Skip header
            url = row[0]
            if url and "book-rooms-" in url and "'" not in url and "+" not in url:
                # Normalize to https
                if url.startswith("http://"):
                    url = "https://" + url[7:]
                urls.add(url)
        
        logger.info(f"Wayback: found {len(urls)} URLs")
    except Exception as e:
        logger.error(f"Wayback error: {e}")
    
    return urls


async def fetch_cc_urls(max_indexes: int = 100) -> Set[str]:
    """Fetch IPMS247 URLs from Common Crawl indexes using Brightdata DC proxy."""
    logger.info(f"Fetching from Common Crawl (max {max_indexes} indexes)...")
    urls = set()
    
    proxy_url = get_brightdata_dc_proxy()
    if not proxy_url:
        logger.error("Brightdata DC proxy not configured - skipping Common Crawl")
        return urls
    
    # Get all available indexes
    async with httpx.AsyncClient(proxy=proxy_url, verify=False, timeout=60) as client:
        try:
            resp = await client.get(f"{CC_INDEX_URL}/collinfo.json")
            resp.raise_for_status()
            indexes = [idx["cdx-api"] for idx in resp.json()][:max_indexes]
            logger.info(f"Found {len(indexes)} CC indexes to query")
        except Exception as e:
            logger.error(f"Failed to get CC indexes: {e}")
            return urls
    
    async def query_index(index_url: str) -> Set[str]:
        found = set()
        params = {
            "url": "live.ipms247.com/booking/book-rooms-*",
            "output": "json",
            "limit": 50000,
        }
        try:
            async with httpx.AsyncClient(proxy=proxy_url, verify=False, timeout=120) as client:
                resp = await client.get(index_url, params=params)
                if resp.status_code == 200:
                    for line in resp.text.strip().split("\n"):
                        if line:
                            try:
                                data = json.loads(line)
                                url = data.get("url", "")
                                if url and "book-rooms-" in url:
                                    # Normalize to https
                                    if url.startswith("http://"):
                                        url = "https://" + url[7:]
                                    found.add(url)
                            except json.JSONDecodeError:
                                pass
        except Exception as e:
            logger.debug(f"CC index {index_url} failed: {e}")
        return found
    
    # Query indexes concurrently (batches of 10)
    for i in range(0, len(indexes), 10):
        batch = indexes[i:i+10]
        results = await asyncio.gather(*[query_index(idx) for idx in batch])
        for result in results:
            urls.update(result)
        logger.info(f"CC progress: {i+len(batch)}/{len(indexes)} indexes, {len(urls)} unique URLs")
    
    logger.info(f"Common Crawl: found {len(urls)} URLs")
    return urls


async def main():
    parser = argparse.ArgumentParser(description="Discover IPMS247 slugs from web archives")
    parser.add_argument("--output", type=str, default="ipms247_slugs.txt", help="Output file")
    parser.add_argument("--s3-upload", action="store_true", help="Upload to S3")
    parser.add_argument("--cc-indexes", type=int, default=50, help="Max Common Crawl indexes to query")
    args = parser.parse_args()
    
    async with httpx.AsyncClient() as client:
        # Fetch from both sources
        wayback_urls = await fetch_wayback_urls(client)
        cc_urls = await fetch_cc_urls(args.cc_indexes)
        
        # Combine and dedupe
        all_urls = wayback_urls | cc_urls
        logger.info(f"Total unique URLs: {len(all_urls)}")
        
        # Write to file
        sorted_urls = sorted(all_urls)
        with open(args.output, "w") as f:
            f.write("\n".join(sorted_urls))
        logger.info(f"Wrote {len(sorted_urls)} URLs to {args.output}")
        
        # Upload to S3 if requested
        if args.s3_upload:
            import aioboto3
            session = aioboto3.Session()
            s3_key = f"crawl-data/ipms247_urls.txt"
            async with session.client("s3", region_name=os.getenv("AWS_REGION", "eu-north-1")) as s3:
                await s3.put_object(
                    Bucket="sadie-gtm",
                    Key=s3_key,
                    Body="\n".join(sorted_urls).encode(),
                )
            logger.info(f"Uploaded to s3://sadie-gtm/{s3_key}")


if __name__ == "__main__":
    asyncio.run(main())
