"""CLI for generating deep-link booking URLs.

Usage:
  # From URL (no DB needed):
  uv run python -m workflows.generate_deeplink \
      --url "https://direct-book.com/properties/thehindsheaddirect" \
      --checkin 2026-03-01 --checkout 2026-03-03 --adults 2

  # From hotel ID (requires DB):
  uv run python -m workflows.generate_deeplink \
      --hotel-id 12345 --checkin 2026-03-01 --checkout 2026-03-03
"""

import argparse
import asyncio
import sys
from datetime import date, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.deeplink.generator import generate_deeplink, generate_deeplink_for_hotel
from lib.deeplink.models import DeepLinkRequest


def main():
    parser = argparse.ArgumentParser(
        description="Generate deep-link booking URLs with dates pre-filled",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --url "https://direct-book.com/properties/thehindsheaddirect" --checkin 2026-03-01 --checkout 2026-03-03
  %(prog)s --url "https://hotels.cloudbeds.com/reservation/kypwgi" --checkin 2026-07-01 --checkout 2026-07-05
  %(prog)s --url "https://app.mews.com/distributor/cb6072cc-1e03-45cc-a6e8-ab0d00ea7979" --checkin 2026-06-01 --checkout 2026-06-03 --promo SUMMER
  %(prog)s --hotel-id 12345 --checkin 2026-03-01 --checkout 2026-03-03
        """,
    )

    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--url", type=str, help="Booking engine URL")
    source.add_argument("--hotel-id", type=int, help="Hotel ID (requires DB)")

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

    args = parser.parse_args()

    checkin = date.fromisoformat(args.checkin)
    checkout = date.fromisoformat(args.checkout)

    if args.url:
        request = DeepLinkRequest(
            booking_url=args.url,
            checkin=checkin,
            checkout=checkout,
            adults=args.adults,
            children=args.children,
            rooms=args.rooms,
            promo_code=args.promo,
        )
        result = generate_deeplink(request)
    else:
        result = asyncio.run(
            generate_deeplink_for_hotel(
                hotel_id=args.hotel_id,
                checkin=checkin,
                checkout=checkout,
                adults=args.adults,
                children=args.children,
                rooms=args.rooms,
                promo_code=args.promo,
            )
        )

    print(f"\nEngine:          {result.engine_name}")
    print(f"Confidence:      {result.confidence.value}")
    print(f"Dates prefilled: {result.dates_prefilled}")
    print(f"Original URL:    {result.original_url}")
    print(f"\nDeep-link URL:\n  {result.url}")


if __name__ == "__main__":
    main()
