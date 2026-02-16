"""CLI for generating deep-link booking URLs.

Usage:
  # Direct link:
  uv run python -m workflows.generate_deeplink \
      --engine resnexus \
      --property-id "a1b2c3d4-e5f6-7890-abcd-ef1234567890" \
      --checkin 2026-03-01 --checkout 2026-03-03 --adults 2

  # Proxy session:
  uv run python -m workflows.generate_deeplink \
      --engine resnexus \
      --property-id "a1b2c3d4-e5f6-7890-abcd-ef1234567890" \
      --checkin 2026-03-01 --checkout 2026-03-03 --proxy
"""

import argparse
import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.deeplink.service import create_direct_link, create_proxy_session


async def main():
    parser = argparse.ArgumentParser(
        description="Generate deep-link booking URLs with dates pre-filled",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --engine resnexus --property-id "a1b2c3d4-..." --checkin 2026-03-01 --checkout 2026-03-03
  %(prog)s --engine cloudbeds --property-id kypwgi --checkin 2026-07-01 --checkout 2026-07-05 --proxy
  %(prog)s --engine siteminder --property-id thehindsheaddirect --checkin 2026-03-01 --checkout 2026-03-03
        """,
    )

    parser.add_argument("--engine", type=str, required=True,
                        choices=["resnexus", "cloudbeds", "siteminder", "mews", "rms"],
                        help="Booking engine name")
    parser.add_argument("--property-id", type=str, required=True,
                        help="Property slug, GUID, or code (engine-specific)")

    default_checkin = date.today() + timedelta(weeks=2)
    default_checkout = default_checkin + timedelta(days=2)

    parser.add_argument("--checkin", type=str, default=default_checkin.isoformat(),
                        help=f"Check-in date YYYY-MM-DD (default: {default_checkin})")
    parser.add_argument("--checkout", type=str, default=default_checkout.isoformat(),
                        help=f"Check-out date YYYY-MM-DD (default: {default_checkout})")
    parser.add_argument("--adults", type=int, default=2, help="Number of adults (default: 2)")
    parser.add_argument("--children", type=int, default=0, help="Number of children (default: 0)")
    parser.add_argument("--rooms", type=int, default=1, help="Number of rooms (default: 1)")
    parser.add_argument("--promo", type=str, default=None, help="Promo/voucher code")
    parser.add_argument("--rate-id", type=str, default=None, help="Room type / rate ID")
    parser.add_argument("--currency", type=str, default=None, help="Currency code (e.g. usd)")
    parser.add_argument("--proxy", action="store_true", help="Create proxy session (Tier 2)")
    parser.add_argument("--proxy-host", type=str, default="localhost:8000",
                        help="Proxy host for session URLs")

    args = parser.parse_args()

    checkin = date.fromisoformat(args.checkin)
    checkout = date.fromisoformat(args.checkout)

    if args.proxy:
        result = await create_proxy_session(
            engine=args.engine,
            property_id=args.property_id,
            checkin=checkin,
            checkout=checkout,
            adults=args.adults,
            children=args.children,
            rooms=args.rooms,
            promo_code=args.promo,
            rate_id=args.rate_id,
            currency=args.currency,
            proxy_host=args.proxy_host,
        )
    else:
        result = create_direct_link(
            engine=args.engine,
            property_id=args.property_id,
            checkin=checkin,
            checkout=checkout,
            adults=args.adults,
            children=args.children,
            rooms=args.rooms,
            promo_code=args.promo,
            rate_id=args.rate_id,
            currency=args.currency,
        )

    print(f"\nEngine:          {result.engine_name}")
    print(f"Confidence:      {result.confidence.value}")
    print(f"Dates prefilled: {result.dates_prefilled}")
    if result.session_id:
        print(f"Session ID:      {result.session_id}")
    print(f"\nDeep-link URL:\n  {result.url}")


if __name__ == "__main__":
    asyncio.run(main())
