"""Automatic API endpoint discovery for booking engines.

Loads a booking engine URL with Playwright, intercepts all API calls the
frontend makes, and outputs a structured report. Use this instead of
manually reading Chrome DevTools Network tab.

Usage:
    # Basic discovery
    uv run python -m workflows.discover_api https://direct-book.com/properties/somehotel

    # With interaction (fill dates, trigger search)
    uv run python -m workflows.discover_api https://direct-book.com/properties/somehotel \
        --interact --checkin 2026-03-01 --checkout 2026-03-03

    # Save HAR file (for mitmproxy2swagger or har2requests)
    uv run python -m workflows.discover_api https://direct-book.com/properties/somehotel \
        --har discovery.har

    # Save JSON report
    uv run python -m workflows.discover_api https://direct-book.com/properties/somehotel \
        --output report.json

    # Watch it work (non-headless)
    uv run python -m workflows.discover_api https://direct-book.com/properties/somehotel \
        --headed
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
from loguru import logger

from lib.api_discovery import ApiDiscoverer


async def run(args):
    discoverer = ApiDiscoverer(
        headless=not args.headed,
        timeout=args.timeout,
        extra_wait=args.wait,
    )

    report = await discoverer.discover(
        url=args.url,
        interact=args.interact,
        checkin=args.checkin,
        checkout=args.checkout,
        adults=args.adults,
        har_path=args.har,
    )

    report.print_report()

    if args.output:
        Path(args.output).write_text(report.to_json())
        logger.info(f"JSON report saved to {args.output}")


def main():
    parser = argparse.ArgumentParser(
        description="Discover API endpoints used by a booking engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s https://direct-book.com/properties/somehotel
  %(prog)s https://hotels.cloudbeds.com/reservation/aBcDeF --interact
  %(prog)s https://app.mews.com/distributor/some-uuid --har mews.har
  %(prog)s https://app.resnexus.com/Rez/Index/12345 --interact --headed
        """,
    )
    parser.add_argument("url", help="Booking engine URL to discover")
    parser.add_argument("--interact", action="store_true",
                        help="Attempt to fill dates and trigger search")
    parser.add_argument("--checkin", type=str, default=None,
                        help="Check-in date YYYY-MM-DD (default: 2 weeks out)")
    parser.add_argument("--checkout", type=str, default=None,
                        help="Check-out date YYYY-MM-DD (default: checkin + 2)")
    parser.add_argument("--adults", type=int, default=2,
                        help="Number of adults (default: 2)")
    parser.add_argument("--har", type=str, default=None,
                        help="Save HAR file to this path")
    parser.add_argument("--output", type=str, default=None,
                        help="Save JSON report to this path")
    parser.add_argument("--headed", action="store_true",
                        help="Run with visible browser (non-headless)")
    parser.add_argument("--timeout", type=int, default=30000,
                        help="Page load timeout in ms (default: 30000)")
    parser.add_argument("--wait", type=float, default=5.0,
                        help="Extra wait time in seconds for lazy API calls (default: 5)")

    args = parser.parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
