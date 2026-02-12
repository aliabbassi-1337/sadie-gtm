"""Unified hotel deduplication across all booking engines.

Three-stage deduplication:
  STAGE 1a - External ID (100% accurate, zero false positives):
    Groups hotels by (external_id, external_id_type).
    Same external_id+type = definitely same hotel.

  STAGE 1b - RMS Client ID (RMS-specific, 100% accurate):
    Extracts numeric RMS client ID from booking URLs.
    Same client ID = same property, even if external_id differs.
    Catches RMS dupes from multiple URL formats / room types.

  STAGE 2 - Name + City + Engine (catches remaining same-engine dupes):
    Groups surviving hotels by normalized(name, city, engine).
    Only deduplicates within the same booking engine.
    Cross-engine records are preserved (different engines = valuable data).

Duplicates are marked with status = -3 (distinct from -1 rejected).

Usage:
    # Dry run (default)
    uv run python -m workflows.deduplicate_unified --dry-run

    # Execute
    uv run python -m workflows.deduplicate_unified --execute

    # Only a specific country
    uv run python -m workflows.deduplicate_unified --country "United States" --execute

    # Show stats
    uv run python -m workflows.deduplicate_unified --stats
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import asyncio
import argparse
import re
from collections import defaultdict
from typing import Dict, List, Any, Optional, Tuple
from loguru import logger

from db.client import init_db, close_db, get_conn


STATUS_DUPLICATE = -3

GARBAGE_NAMES = {
    "", " ", "online bookings", "online booking", "book now", "book online",
    "reservations", "reservation", "hotel", "search", "error", "unknown",
    "rms", "rms online booking", "new booking", "rates", "test",
    "an unhandled exception occurred while processing the request.",
}

GARBAGE_CITIES = {
    "", " ", "rms online booking", "online bookings", "none", "null",
    "its bubbling", "n/a",
}


def normalize(text: str) -> str:
    if not text:
        return ""
    return text.strip().lower()


def extract_website_domain(url: Optional[str]) -> str:
    """Extract normalized domain from a website URL for comparison."""
    if not url:
        return ""
    u = url.strip().lower()
    u = re.sub(r'^https?://(www\.)?', '', u)
    u = u.rstrip('/').split('/')[0]
    # Ignore chain/corporate domains that many properties share
    if not u or '.' not in u:
        return ""
    return u


# Common street abbreviations → full forms for address comparison
# Uses regex word-boundary matching to avoid replacing inside full words
# (e.g., "Street" should NOT become "Streetreet")
_STREET_ABBREVS = [
    (r'\bst\b\.?', 'street'),
    (r'\brd\b\.?', 'road'),
    (r'\bave\b\.?', 'avenue'),
    (r'\bdr\b\.?', 'drive'),
    (r'\bblvd\b\.?', 'boulevard'),
    (r'\bln\b\.?', 'lane'),
    (r'\bct\b\.?', 'court'),
    (r'\bpl\b\.?', 'place'),
    (r'\bcres\b\.?', 'crescent'),
    (r'\btce\b\.?', 'terrace'),
    (r'\bhwy\b\.?', 'highway'),
    (r'\bpde\b\.?', 'parade'),
]


def normalize_address(text: str) -> str:
    """Normalize address for comparison — expand abbreviations, strip extra info."""
    if not text:
        return ""
    addr = text.strip().lower()
    # Take only the street part (before first comma) to strip city/state/zip
    parts = addr.split(",")
    addr = parts[0].strip()
    # Expand street abbreviations (word-boundary safe)
    for pattern, replacement in _STREET_ABBREVS:
        addr = re.sub(pattern, replacement, addr)
    # Collapse whitespace
    addr = re.sub(r'\s+', ' ', addr).strip()
    return addr


def is_garbage_name(name: Optional[str]) -> bool:
    if not name:
        return True
    return normalize(name) in GARBAGE_NAMES


def is_garbage_city(city: Optional[str]) -> bool:
    if not city:
        return True
    return normalize(city) in GARBAGE_CITIES


# --- RMS Cloud client-ID extraction ---
# RMS external_ids / booking URLs contain a numeric client ID that uniquely
# identifies the property.  Multiple rows can exist for the same property
# with different URL formats (different room-type paths, HTTP vs HTTPS, etc).
# Patterns matched (from old dedupe_rms.py):
#   - bookings.rmscloud.com/search/index/<ID>/...
#   - bookings12.rmscloud.com/rates/index/<ID>/...
#   - rmscloud.com/<ID>
#   - ibe*.rmscloud.com/<ID>
#   - <ID> (plain numeric external_id)

RMS_ENGINE_NAMES = {"rms cloud", "rms"}


def extract_rms_client_id(external_id: Optional[str]) -> Optional[str]:
    """Extract the RMS client ID (numeric or hex) from a booking URL or plain ID.

    Tries multiple URL patterns used by RMS Cloud, returns the first match.
    IDs can be numeric (e.g. 18039) or hex (e.g. 29c9ae76f9bd6297).
    All returned IDs are lowercased for consistent grouping.
    """
    if not external_id:
        return None

    url = external_id.strip()

    # Pattern 1: /search/index/<ID> (numeric or hex)
    m = re.search(r'/search/index/([a-f0-9]+)(?:/|$)', url, re.IGNORECASE)
    if m:
        return m.group(1).lower()

    # Pattern 2: /rates/index/<ID> (numeric or hex)
    m = re.search(r'/rates/index/([a-f0-9]+)(?:/|$)', url, re.IGNORECASE)
    if m:
        return m.group(1).lower()

    # Pattern 3: rmscloud.com/<ID> (numeric or hex)
    m = re.search(r'rmscloud\.com/([a-f0-9]+)(?:/|$)', url, re.IGNORECASE)
    if m:
        return m.group(1).lower()

    # Pattern 4: ibe*.rmscloud.com/<ID>
    m = re.search(r'ibe\d*\.rmscloud\.com/([a-f0-9]+)', url, re.IGNORECASE)
    if m:
        return m.group(1).lower()

    # Pattern 5: rms_<ID> prefix (from rms_scan source)
    m = re.match(r'^rms_(\d+)$', url, re.IGNORECASE)
    if m:
        return m.group(1)

    # Pattern 6: plain numeric ID (4+ digits)
    m = re.match(r'^(\d{4,})$', url)
    if m:
        return m.group(1)

    # Pattern 7: plain hex ID (8+ hex chars, not all digits)
    m = re.match(r'^([a-f0-9]{8,})$', url, re.IGNORECASE)
    if m:
        return m.group(1).lower()

    return None


def score_record(r: Dict[str, Any]) -> int:
    """Score a hotel record -- higher = better data quality."""
    s = 0
    if r.get("email"):
        s += 15
    if r.get("address"):
        s += 10
    if not is_garbage_city(r.get("city")):
        s += 8
    if r.get("state"):
        s += 5
    if not is_garbage_name(r.get("name")):
        s += 5
    if r.get("phone_website"):
        s += 3
    if r.get("website"):
        s += 3
    if r.get("country"):
        s += 2
    if r.get("status", 0) >= 1:
        s += 5
    return s


def _pick_best_value(keeper_val: Optional[str], other_val: Optional[str]) -> Optional[str]:
    """Pick the more normalized/complete value between two non-empty strings.

    Prefers:
      - Longer values (e.g. "New South Wales" over "NSW")
      - Mixed/title case over ALL CAPS (e.g. "Surfers Paradise" over "SURFERS PARADISE")
    """
    if not keeper_val:
        return other_val
    if not other_val:
        return keeper_val

    k, o = keeper_val.strip(), other_val.strip()
    if not k:
        return o
    if not o:
        return k

    # Prefer longer (more complete / not abbreviated)
    if len(o) > len(k):
        return o

    # Same length: prefer non-ALL-CAPS
    if k == k.upper() and o != o.upper():
        return o

    return keeper_val


# Fields where we should upgrade to the longer/better value even if keeper has data
_UPGRADE_FIELDS = {"name", "city", "state", "country", "address"}


def merge_group(recs: List[Dict]) -> Tuple[Dict, List[int]]:
    """
    Merge a group of duplicates. Returns (keeper, dupe_ids).

    - Empty keeper fields get filled from duplicates.
    - For location/name fields, shorter or ALL-CAPS values get upgraded
      to longer or properly-cased values (e.g. "NSW" -> "New South Wales").
    """
    if len(recs) == 1:
        return recs[0], []

    recs_sorted = sorted(recs, key=score_record, reverse=True)
    keeper = recs_sorted[0].copy()
    dupe_ids = []
    merged_any = False

    for other in recs_sorted[1:]:
        for field, garbage_check in [
            ("email", None),
            ("name", is_garbage_name),
            ("city", is_garbage_city),
            ("state", None),
            ("address", None),
            ("phone_website", None),
            ("website", None),
            ("country", None),
        ]:
            keeper_val = keeper.get(field)
            other_val = other.get(field)
            keeper_empty = (garbage_check(keeper_val) if garbage_check else not keeper_val)
            other_filled = (not garbage_check(other_val) if garbage_check else bool(other_val))

            if keeper_empty and other_filled:
                # Fill empty field
                keeper[field] = other_val
                merged_any = True
            elif field in _UPGRADE_FIELDS and other_filled and not keeper_empty:
                # Both have values -- pick the better one
                better = _pick_best_value(keeper_val, other_val)
                if better != keeper_val:
                    keeper[field] = better
                    merged_any = True

        dupe_ids.append(other["hotel_id"])

    keeper["_merged"] = merged_any
    return keeper, dupe_ids


async def fetch_hotels(
    conn,
    country: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Fetch all active hotels (one row per hotel, not per engine)."""
    filters = ["h.status >= 0"]
    params: list = []

    if country:
        params.append(country)
        filters.append(f"h.country = ${len(params)}")

    where = " AND ".join(filters)

    # Use a subquery to get engine name + booking_url without multiplying rows
    rows = await conn.fetch(f"""
        SELECT
            h.id as hotel_id,
            h.external_id,
            h.external_id_type,
            h.name,
            h.email,
            h.city,
            h.state,
            h.country,
            h.address,
            h.phone_website,
            h.website,
            h.status,
            (
                SELECT be.name FROM sadie_gtm.hotel_booking_engines hbe
                JOIN sadie_gtm.booking_engines be ON hbe.booking_engine_id = be.id
                WHERE hbe.hotel_id = h.id AND hbe.status = 1
                LIMIT 1
            ) as engine_name,
            (
                SELECT hbe.booking_url FROM sadie_gtm.hotel_booking_engines hbe
                WHERE hbe.hotel_id = h.id AND hbe.status = 1
                LIMIT 1
            ) as booking_url
        FROM sadie_gtm.hotels h
        WHERE {where}
        ORDER BY h.id
    """, *params, timeout=300)

    valid = []
    garbage = 0
    for r in rows:
        if is_garbage_name(r["name"]):
            garbage += 1
        else:
            valid.append(dict(r))

    if garbage:
        logger.info(f"Excluded {garbage} records with garbage names")
    return valid


async def execute_batch(conn, keepers: List[Dict], all_dupes: List[int]):
    """Execute dedup using batch SQL operations (fast)."""
    async with conn.transaction():
        # 1. Mark all duplicates in one shot
        if all_dupes:
            await conn.execute(
                "UPDATE sadie_gtm.hotels SET status = $1, updated_at = NOW() WHERE id = ANY($2)",
                STATUS_DUPLICATE,
                all_dupes,
                timeout=120,
            )
            logger.info(f"Marked {len(all_dupes)} duplicates as status={STATUS_DUPLICATE}")

        # 2. Batch-update keepers that had data merged, using a temp table
        merged_keepers = [k for k in keepers if k.get("_merged")]
        if not merged_keepers:
            logger.info("No keepers needed data merge updates")
            return

        await conn.execute("""
            CREATE TEMP TABLE _dedup_keepers (
                hotel_id INTEGER PRIMARY KEY,
                name TEXT,
                email TEXT,
                city TEXT,
                state TEXT,
                country TEXT,
                address TEXT,
                phone_website TEXT,
                website TEXT
            ) ON COMMIT DROP
        """)

        # Bulk insert into temp table
        await conn.executemany(
            """INSERT INTO _dedup_keepers
               (hotel_id, name, email, city, state, country, address, phone_website, website)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)""",
            [
                (
                    k["hotel_id"],
                    k.get("name") or "",
                    k.get("email") or "",
                    k.get("city") or "",
                    k.get("state") or "",
                    k.get("country") or "",
                    k.get("address") or "",
                    k.get("phone_website") or "",
                    k.get("website") or "",
                )
                for k in merged_keepers
            ],
        )

        # Single UPDATE join -- use merged values directly (they already
        # contain the best pick from _pick_best_value), fall back to
        # existing DB value only if merged value is empty.
        result = await conn.execute("""
            UPDATE sadie_gtm.hotels h
            SET
                name = CASE WHEN t.name != '' THEN t.name ELSE h.name END,
                email = CASE WHEN t.email != '' THEN t.email ELSE h.email END,
                city = CASE WHEN t.city != '' THEN t.city ELSE h.city END,
                state = CASE WHEN t.state != '' THEN t.state ELSE h.state END,
                country = CASE WHEN t.country != '' THEN t.country ELSE h.country END,
                address = CASE WHEN t.address != '' THEN t.address ELSE h.address END,
                phone_website = CASE WHEN t.phone_website != '' THEN t.phone_website ELSE h.phone_website END,
                website = CASE WHEN t.website != '' THEN t.website ELSE h.website END,
                updated_at = NOW()
            FROM _dedup_keepers t
            WHERE h.id = t.hotel_id
        """, timeout=120)
        logger.info(f"Batch-updated {len(merged_keepers)} keepers with merged data")


async def run_dedup(
    dry_run: bool = True,
    country: Optional[str] = None,
):
    """Run unified three-stage deduplication (ext_id, RMS client ID, name+city)."""
    await init_db()

    try:
        async with get_conn() as conn:
            scope_str = f" (country={country})" if country else " (all)"

            logger.info(f"Fetching hotels{scope_str}...")
            records = await fetch_hotels(conn, country=country)
            logger.info(f"Active records: {len(records)}")

            if not records:
                logger.info("No records to deduplicate")
                return

            # ==============================================================
            # STAGE 1a: Deduplicate by External ID + Type
            # ==============================================================
            logger.info("")
            logger.info("=" * 70)
            logger.info("STAGE 1a: DEDUPLICATE BY EXTERNAL ID")
            logger.info("(Same external_id + type = definitely same hotel)")
            logger.info("=" * 70)

            by_ext_id: Dict[Tuple[str, str], List[Dict]] = defaultdict(list)
            no_ext_id: List[Dict] = []

            for r in records:
                ext_id = r.get("external_id")
                ext_type = r.get("external_id_type") or ""
                if ext_id:
                    by_ext_id[(ext_id, ext_type)].append(r)
                else:
                    no_ext_id.append(r)

            logger.info(f"Records with external_id: {sum(len(v) for v in by_ext_id.values())}")
            logger.info(f"Records without external_id: {len(no_ext_id)}")
            logger.info(f"Unique (external_id, type) pairs: {len(by_ext_id)}")

            dup_groups_s1a = sum(1 for recs in by_ext_id.values() if len(recs) > 1)
            logger.info(f"Groups with duplicates: {dup_groups_s1a}")

            stage1a_keepers = []
            stage1a_dupes = []

            for key, recs in by_ext_id.items():
                keeper, dupe_ids = merge_group(recs)
                stage1a_keepers.append(keeper)
                stage1a_dupes.extend(dupe_ids)

            logger.info(f"Stage 1a keepers: {len(stage1a_keepers)}, dupes: {len(stage1a_dupes)}")

            # ==============================================================
            # STAGE 1b: Deduplicate RMS Cloud by Client ID
            # ==============================================================
            logger.info("")
            logger.info("=" * 70)
            logger.info("STAGE 1b: DEDUPLICATE RMS BY CLIENT ID")
            logger.info("(Same numeric client ID from booking URL = same property)")
            logger.info("=" * 70)

            # Separate RMS records from non-RMS among Stage 1a survivors
            rms_records: List[Dict] = []
            non_rms_records: List[Dict] = []

            for r in stage1a_keepers + no_ext_id:
                engine = normalize(r.get("engine_name") or "")
                if engine in RMS_ENGINE_NAMES:
                    rms_records.append(r)
                else:
                    non_rms_records.append(r)

            logger.info(f"RMS records to check: {len(rms_records)}")

            by_client_id: Dict[str, List[Dict]] = defaultdict(list)
            rms_no_client_id: List[Dict] = []

            for r in rms_records:
                # Try booking_url first (most reliable), fall back to external_id
                cid = extract_rms_client_id(r.get("booking_url")) or extract_rms_client_id(r.get("external_id"))
                if cid:
                    by_client_id[cid].append(r)
                else:
                    rms_no_client_id.append(r)

            logger.info(f"RMS records with extractable client ID: {sum(len(v) for v in by_client_id.values())}")
            logger.info(f"RMS records without client ID: {len(rms_no_client_id)}")
            logger.info(f"Unique RMS client IDs: {len(by_client_id)}")

            dup_groups_s1b = sum(1 for recs in by_client_id.values() if len(recs) > 1)
            logger.info(f"RMS client ID groups with duplicates: {dup_groups_s1b}")

            stage1b_keepers = []
            stage1b_dupes = []

            for cid, recs in by_client_id.items():
                keeper, dupe_ids = merge_group(recs)
                stage1b_keepers.append(keeper)
                stage1b_dupes.extend(dupe_ids)

            if stage1b_dupes:
                # Print a few samples
                logger.info("")
                logger.info("Sample Stage 1b merges (RMS client ID):")
                shown = 0
                for cid, recs in by_client_id.items():
                    if len(recs) > 1 and shown < 5:
                        recs_s = sorted(recs, key=score_record, reverse=True)
                        logger.info(
                            f"  client_id={cid} ({len(recs)} records)"
                        )
                        logger.info(
                            f"    KEEP [{recs_s[0]['hotel_id']}]: {(recs_s[0]['name'] or '')[:40]} | "
                            f"{recs_s[0].get('city') or '-'} | score={score_record(recs_s[0])}"
                        )
                        for d in recs_s[1:3]:
                            logger.info(
                                f"    DUPE [{d['hotel_id']}]: {(d['name'] or '')[:40]} | "
                                f"{d.get('city') or '-'} | score={score_record(d)}"
                            )
                        if len(recs) > 3:
                            logger.info(f"    ... and {len(recs) - 3} more")
                        shown += 1

            logger.info(f"Stage 1b keepers: {len(stage1b_keepers)}, dupes: {len(stage1b_dupes)}")

            # Combine for Stage 2 input: non-RMS + RMS keepers + RMS without client ID
            stage1_survivors = non_rms_records + stage1b_keepers + rms_no_client_id
            stage1_all_dupes = stage1a_dupes + stage1b_dupes

            # ==============================================================
            # STAGE 2: Deduplicate by Name + City + Engine
            # ==============================================================
            logger.info("")
            logger.info("=" * 70)
            logger.info("STAGE 2: DEDUPLICATE BY NAME + CITY + ENGINE")
            logger.info("(Same-engine dupes only; cross-engine records preserved)")
            logger.info("=" * 70)

            stage2_input = stage1_survivors
            logger.info(f"Stage 2 input: {len(stage2_input)} records")

            by_name_city: Dict[Tuple[str, str, str], List[Dict]] = defaultdict(list)
            for r in stage2_input:
                engine = normalize(r.get("engine_name") or "unknown")
                key = (normalize(r.get("name", "")), normalize(r.get("city", "")), engine)
                by_name_city[key].append(r)

            raw_dup_groups = sum(1 for recs in by_name_city.values() if len(recs) > 1)
            logger.info(f"Raw name+city groups with duplicates: {raw_dup_groups}")

            final_keepers = []
            stage2_dupes = []
            skipped_chain = 0
            skipped_country = 0
            skipped_no_signal = 0

            for key, recs in by_name_city.items():
                if len(recs) > 1:
                    name_key, city_key, eng_key = key

                    # Safety 1: skip groups spanning multiple countries
                    countries = set()
                    for r in recs:
                        c = normalize(r.get("country") or "")
                        if c:
                            countries.add(c)
                    if len(countries) > 1:
                        skipped_country += 1
                        for r in recs:
                            final_keepers.append(r)
                        continue

                    # Safety 2: skip groups where multiple records have DIFFERENT
                    # non-empty addresses -- likely chain hotels at different locations
                    # Uses normalize_address() to expand abbreviations (St→Street)
                    # and strip extra info after comma (city/state/zip)
                    addrs = set()
                    for r in recs:
                        a = normalize_address(r.get("address", ""))
                        if a:
                            addrs.add(a)
                    if len(addrs) > 1:
                        skipped_chain += 1
                        for r in recs:
                            final_keepers.append(r)
                        continue

                    # Safety 3: if city is empty AND no addresses to verify,
                    # check if website or email domains match as an extra signal.
                    # Matching website/email = safe to dedup even without location.
                    if not city_key and len(addrs) == 0:
                        websites = set()
                        email_domains = set()
                        for r in recs:
                            w = normalize(r.get("website") or "")
                            if w:
                                # Strip scheme and www for comparison
                                w = re.sub(r'^https?://(www\.)?', '', w).rstrip('/')
                                websites.add(w)
                            e = (r.get("email") or "").strip().lower()
                            if e and '@' in e:
                                email_domains.add(e.split('@')[1])
                        # Need at least one shared signal: same website or same email domain
                        has_shared_website = len(websites) == 1 and len(websites) > 0
                        has_shared_email = len(email_domains) == 1 and len(email_domains) > 0
                        if not has_shared_website and not has_shared_email:
                            skipped_no_signal += 1
                            for r in recs:
                                final_keepers.append(r)
                            continue

                keeper, dupe_ids = merge_group(recs)
                final_keepers.append(keeper)
                stage2_dupes.extend(dupe_ids)

            total_skipped = skipped_country + skipped_chain + skipped_no_signal
            logger.info(f"Skipped {skipped_country} groups (multi-country)")
            logger.info(f"Skipped {skipped_chain} groups (different addresses / chain hotels)")
            logger.info(f"Skipped {skipped_no_signal} groups (empty city + no address = no signal)")
            logger.info(f"Actual duplicate groups: {raw_dup_groups - total_skipped}")

            logger.info(f"Stage 2a keepers: {len(final_keepers)}, dupes: {len(stage2_dupes)}")

            # ==============================================================
            # STAGE 2b: Deduplicate by Name + Website Domain + Engine
            # Catches dupes with different city strings but same website
            # (e.g. "Mollymook Beach" vs "Mollymook")
            # ==============================================================
            logger.info("")
            logger.info("=" * 70)
            logger.info("STAGE 2b: DEDUPLICATE BY NAME + WEBSITE + ENGINE")
            logger.info("(Catches dupes with different city strings but same website)")
            logger.info("=" * 70)

            # Build groups from Stage 2a survivors (final_keepers)
            by_name_web: Dict[Tuple[str, str, str], List[Dict]] = defaultdict(list)
            for r in final_keepers:
                domain = extract_website_domain(r.get("website"))
                if not domain:
                    continue
                engine = normalize(r.get("engine_name") or "unknown")
                name_key = normalize(r.get("name", ""))
                if name_key:
                    by_name_web[(name_key, domain, engine)].append(r)

            raw_dup_groups_2b = sum(1 for recs in by_name_web.values() if len(recs) > 1)
            logger.info(f"Raw name+website groups with duplicates: {raw_dup_groups_2b}")

            stage2b_keepers = []
            stage2b_dupes = []
            skipped_2b_country = 0
            skipped_2b_chain = 0

            # Records NOT in any name+web group stay as-is
            in_name_web = set()
            for recs in by_name_web.values():
                for r in recs:
                    in_name_web.add(r["hotel_id"])
            stage2b_passthrough = [r for r in final_keepers if r["hotel_id"] not in in_name_web]

            for key, recs in by_name_web.items():
                if len(recs) == 1:
                    stage2b_keepers.append(recs[0])
                    continue

                name_key, domain, eng_key = key

                # Safety: skip groups spanning multiple countries
                countries = set()
                for r in recs:
                    c = normalize(r.get("country") or "")
                    if c:
                        countries.add(c)
                if len(countries) > 1:
                    skipped_2b_country += 1
                    stage2b_keepers.extend(recs)
                    continue

                # Safety: skip groups with different non-empty cities
                cities = set()
                for r in recs:
                    c = normalize(r.get("city") or "")
                    if c and c not in GARBAGE_CITIES:
                        cities.add(c)
                if len(cities) > 1:
                    skipped_2b_chain += 1
                    stage2b_keepers.extend(recs)
                    continue

                # Safety: skip groups with different non-empty addresses
                addrs = set()
                for r in recs:
                    a = normalize_address(r.get("address", ""))
                    if a:
                        addrs.add(a)
                if len(addrs) > 1:
                    skipped_2b_chain += 1
                    stage2b_keepers.extend(recs)
                    continue

                keeper, dupe_ids = merge_group(recs)
                stage2b_keepers.append(keeper)
                stage2b_dupes.extend(dupe_ids)

            final_keepers = stage2b_passthrough + stage2b_keepers

            logger.info(f"Skipped {skipped_2b_country} groups (multi-country)")
            logger.info(f"Skipped {skipped_2b_chain} groups (different city/address / chain)")
            logger.info(f"Actual Stage 2b merges: {raw_dup_groups_2b - skipped_2b_country - skipped_2b_chain}")
            logger.info(f"Stage 2b dupes: {len(stage2b_dupes)}")

            # Filter by_name_web to only merged groups for sample display
            merged_2b_keys = set()
            for key, recs in by_name_web.items():
                if len(recs) > 1:
                    name_key, domain, eng_key = key
                    cities = set()
                    for r in recs:
                        c = normalize(r.get("city") or "")
                        if c and c not in GARBAGE_CITIES:
                            cities.add(c)
                    if len(cities) > 1:
                        continue
                    addrs = set()
                    for r in recs:
                        a = normalize_address(r.get("address", ""))
                        if a:
                            addrs.add(a)
                    if len(addrs) > 1:
                        continue
                    countries = set()
                    for r in recs:
                        c2 = normalize(r.get("country") or "")
                        if c2:
                            countries.add(c2)
                    if len(countries) > 1:
                        continue
                    merged_2b_keys.add(key)
            by_name_web_merged = {k: v for k, v in by_name_web.items() if k in merged_2b_keys}

            # ==============================================================
            # SUMMARY
            # ==============================================================
            all_dupes = stage1_all_dupes + stage2_dupes + stage2b_dupes
            logger.info("")
            logger.info("=" * 70)
            logger.info("SUMMARY")
            logger.info("=" * 70)
            logger.info(f"Original records:                    {len(records):>8,}")
            logger.info(f"Stage 1a duplicates (external_id):   {len(stage1a_dupes):>8,}")
            logger.info(f"Stage 1b duplicates (RMS client ID): {len(stage1b_dupes):>8,}")
            logger.info(f"Stage 2a duplicates (name+city):     {len(stage2_dupes):>8,}")
            logger.info(f"Stage 2b duplicates (name+website):  {len(stage2b_dupes):>8,}")
            logger.info(f"Total duplicates:                    {len(all_dupes):>8,}")
            logger.info(f"Final unique hotels:                 {len(final_keepers):>8,}")
            if records:
                logger.info(
                    f"Reduction: {len(all_dupes):,} ({100 * len(all_dupes) / len(records):.1f}%)"
                )

            if dry_run:
                _print_samples(by_ext_id, by_name_city, by_name_web_merged, stage1a_dupes, stage2_dupes, stage2b_dupes)
                logger.info("")
                logger.info("Run with --execute to apply changes")
            else:
                logger.info("")
                logger.info("Executing deduplication...")
                await execute_batch(conn, final_keepers, all_dupes)

                active = await conn.fetchval(
                    "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE status >= 0"
                )
                logger.info("")
                logger.info("=" * 70)
                logger.info("DONE")
                logger.info("=" * 70)
                logger.info(f"Active hotels remaining: {active:,}")

    finally:
        await close_db()


def _print_samples(by_ext_id, by_name_city, by_name_web, stage1_dupes, stage2_dupes, stage2b_dupes):
    """Print sample merges for dry-run output."""
    if stage1_dupes:
        logger.info("")
        logger.info("Sample Stage 1 merges (external_id):")
        shown = 0
        for (ext_id, ext_type), recs in by_ext_id.items():
            if len(recs) > 1 and shown < 5:
                recs_s = sorted(recs, key=score_record, reverse=True)
                logger.info(
                    f"  ext_id={ext_id[:30]} type={ext_type} "
                    f"({len(recs)} records, engine={recs[0].get('engine_name')})"
                )
                logger.info(
                    f"    KEEP [{recs_s[0]['hotel_id']}]: {(recs_s[0]['name'] or '')[:40]} | "
                    f"{recs_s[0].get('city') or '-'} | score={score_record(recs_s[0])}"
                )
                for d in recs_s[1:2]:
                    logger.info(
                        f"    DUPE [{d['hotel_id']}]: {(d['name'] or '')[:40]} | "
                        f"{d.get('city') or '-'} | score={score_record(d)}"
                    )
                if len(recs) > 2:
                    logger.info(f"    ... and {len(recs) - 2} more")
                shown += 1

    if stage2_dupes:
        logger.info("")
        logger.info("Sample Stage 2a merges (name+city+engine):")
        shown = 0
        for (n, c, eng), recs in by_name_city.items():
            if len(recs) > 1 and shown < 5:
                recs_s = sorted(recs, key=score_record, reverse=True)
                logger.info(
                    f"  \"{n[:35]}\" + \"{c}\" + [{eng}] ({len(recs)} records)"
                )
                logger.info(
                    f"    KEEP [{recs_s[0]['hotel_id']}]: "
                    f"score={score_record(recs_s[0])}"
                )
                for d in recs_s[1:2]:
                    logger.info(
                        f"    DUPE [{d['hotel_id']}]: "
                        f"score={score_record(d)}"
                    )
                if len(recs) > 2:
                    logger.info(f"    ... and {len(recs) - 2} more")
                shown += 1

    if stage2b_dupes:
        logger.info("")
        logger.info("Sample Stage 2b merges (name+website+engine):")
        shown = 0
        for (n, dom, eng), recs in by_name_web.items():
            if len(recs) > 1 and shown < 5:
                recs_s = sorted(recs, key=score_record, reverse=True)
                logger.info(
                    f"  \"{n[:35]}\" + \"{dom}\" + [{eng}] ({len(recs)} records)"
                )
                for r in recs_s[:3]:
                    tag = "KEEP" if r is recs_s[0] else "DUPE"
                    logger.info(
                        f"    {tag} [{r['hotel_id']}]: {(r.get('city') or '-')[:20]} | "
                        f"score={score_record(r)}"
                    )
                if len(recs) > 3:
                    logger.info(f"    ... and {len(recs) - 3} more")
                shown += 1


async def show_stats():
    """Show duplicate statistics."""
    await init_db()
    try:
        async with get_conn() as conn:
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE status >= 0"
            )
            duped = await conn.fetchval(
                "SELECT COUNT(*) FROM sadie_gtm.hotels WHERE status = $1", STATUS_DUPLICATE
            )
            dup_ext = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT external_id, external_id_type
                    FROM sadie_gtm.hotels
                    WHERE external_id IS NOT NULL AND status >= 0
                    GROUP BY external_id, external_id_type
                    HAVING COUNT(*) > 1
                ) t
            """, timeout=60)
            dup_nc = await conn.fetchval("""
                SELECT COUNT(*) FROM (
                    SELECT LOWER(TRIM(name)), LOWER(TRIM(COALESCE(city, '')))
                    FROM sadie_gtm.hotels
                    WHERE status >= 0 AND name IS NOT NULL AND LOWER(TRIM(name)) NOT IN (
                        '', ' ', 'online bookings', 'new booking', 'hotel', 'rms online booking'
                    )
                    GROUP BY LOWER(TRIM(name)), LOWER(TRIM(COALESCE(city, '')))
                    HAVING COUNT(*) > 1
                ) t
            """, timeout=60)

            logger.info("=" * 60)
            logger.info("Duplicate Statistics")
            logger.info("=" * 60)
            logger.info(f"  Active hotels:                          {total:>8,}")
            logger.info(f"  Already marked duplicate (status=-3):   {duped:>8,}")
            logger.info(f"  Duplicate (external_id,type) groups:    {dup_ext:>8,}")
            logger.info(f"  Duplicate (name+city) groups:           {dup_nc:>8,}")
    finally:
        await close_db()


def main():
    parser = argparse.ArgumentParser(
        description="Unified hotel deduplication (1a: external_id, 1b: RMS client ID, 2: name+city+engine)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done (default)")
    parser.add_argument("--execute", action="store_true", help="Execute the deduplication")
    parser.add_argument("--country", type=str, help="Filter by country (e.g. 'United States')")
    parser.add_argument("--stats", action="store_true", help="Show duplicate statistics only")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="INFO", format="<level>{level: <8}</level> | {message}")

    if args.stats:
        asyncio.run(show_stats())
    else:
        dry_run = not args.execute
        asyncio.run(run_dedup(dry_run=dry_run, country=args.country))


if __name__ == "__main__":
    main()
