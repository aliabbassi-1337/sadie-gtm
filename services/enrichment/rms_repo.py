"""RMS Repository - Database operations for RMS enrichment."""

from typing import Optional, List, Dict

from db.client import queries, get_conn
from lib.rms import RMSHotelRecord


class RMSRepo:
    """Database operations for RMS enrichment."""
    
    def __init__(self):
        self._booking_engine_id: Optional[int] = None
    
    async def get_booking_engine_id(self) -> int:
        """Get RMS Cloud booking engine ID from database."""
        if self._booking_engine_id is None:
            async with get_conn() as conn:
                result = await queries.get_rms_booking_engine_id(conn)
                if result:
                    self._booking_engine_id = result["id"]
                else:
                    raise ValueError("RMS Cloud booking engine not found")
        return self._booking_engine_id
    
    async def get_hotels_needing_enrichment(self, limit: int = 1000) -> List[RMSHotelRecord]:
        """Get RMS hotels that need enrichment."""
        booking_engine_id = await self.get_booking_engine_id()
        async with get_conn() as conn:
            results = await queries.get_rms_hotels_needing_enrichment(
                conn, booking_engine_id=booking_engine_id, limit=limit
            )
            return [
                RMSHotelRecord(hotel_id=r["hotel_id"], booking_url=r["booking_url"])
                for r in results
            ]
    
    async def update_hotel(
        self,
        hotel_id: int,
        name: Optional[str] = None,
        address: Optional[str] = None,
        city: Optional[str] = None,
        state: Optional[str] = None,
        country: Optional[str] = None,
        phone: Optional[str] = None,
        email: Optional[str] = None,
        website: Optional[str] = None,
    ) -> None:
        """Update hotel with enriched data."""
        async with get_conn() as conn:
            await queries.update_rms_hotel(
                conn, hotel_id=hotel_id, name=name, address=address, city=city,
                state=state, country=country, phone=phone, email=email, website=website,
            )
    
    async def update_enrichment_status(self, booking_url: str, status: int) -> None:
        """Update enrichment status for a hotel. 1=success, -1=failed."""
        async with get_conn() as conn:
            await queries.update_rms_enrichment_status(conn, booking_url=booking_url, status=status)
    
    async def get_stats(self) -> Dict[str, int]:
        """Get RMS hotel statistics."""
        booking_engine_id = await self.get_booking_engine_id()
        async with get_conn() as conn:
            result = await queries.get_rms_stats(conn, booking_engine_id=booking_engine_id)
            if result:
                return dict(result)
            return {
                "total": 0, "with_name": 0, "with_city": 0, "with_email": 0,
                "with_phone": 0, "enriched": 0, "no_data": 0, "dead": 0,
            }
    
    async def count_needing_enrichment(self) -> int:
        """Count RMS hotels needing enrichment."""
        booking_engine_id = await self.get_booking_engine_id()
        async with get_conn() as conn:
            result = await queries.count_rms_needing_enrichment(conn, booking_engine_id=booking_engine_id)
            return result["count"] if result else 0

    async def batch_update_enrichment(
        self,
        updates: List[Dict],
        failed_urls: List[str],
    ) -> int:
        """Batch update RMS hotels and enrichment status in a single query.
        
        Args:
            updates: List of dicts with hotel_id, booking_url, name, address, etc.
            failed_urls: List of booking_urls that failed enrichment
            
        Returns:
            Number of hotels updated
        """
        if not updates and not failed_urls:
            return 0
        
        updated = 0
        async with get_conn() as conn:
            # Batch update hotels with enriched data
            if updates:
                hotel_ids = [u["hotel_id"] for u in updates]
                names = [u.get("name") for u in updates]
                addresses = [u.get("address") for u in updates]
                cities = [u.get("city") for u in updates]
                states = [u.get("state") for u in updates]
                countries = [u.get("country") for u in updates]
                phones = [u.get("phone") for u in updates]
                emails = [u.get("email") for u in updates]
                websites = [u.get("website") for u in updates]
                latitudes = [u.get("latitude") for u in updates]
                longitudes = [u.get("longitude") for u in updates]
                
                sql_hotels = """
                UPDATE sadie_gtm.hotels h
                SET
                    name = CASE 
                        WHEN h.name IS NULL OR h.name = '' OR h.name LIKE 'Unknown%' 
                             OR h.name = 'Online Bookings' OR h.name LIKE '%rmscloud.com%'
                        THEN COALESCE(v.name, h.name)
                        ELSE h.name 
                    END,
                    address = CASE WHEN h.address IS NULL OR h.address = '' THEN v.address ELSE h.address END,
                    city = CASE WHEN h.city IS NULL OR h.city = '' THEN v.city ELSE h.city END,
                    state = CASE WHEN h.state IS NULL OR h.state = '' THEN v.state ELSE h.state END,
                    country = CASE WHEN h.country IS NULL OR h.country = '' THEN v.country ELSE h.country END,
                    phone_website = CASE WHEN h.phone_website IS NULL OR h.phone_website = '' THEN v.phone ELSE h.phone_website END,
                    email = CASE WHEN h.email IS NULL OR h.email = '' THEN v.email ELSE h.email END,
                    website = CASE WHEN h.website IS NULL OR h.website = '' THEN v.website ELSE h.website END,
                    location = CASE 
                        WHEN h.location IS NULL AND v.latitude IS NOT NULL AND v.longitude IS NOT NULL 
                        THEN ST_SetSRID(ST_MakePoint(v.longitude, v.latitude), 4326)::geography
                        ELSE h.location 
                    END,
                    updated_at = NOW()
                FROM (
                    SELECT * FROM unnest($1::int[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[], $7::text[], $8::text[], $9::text[], $10::float[], $11::float[])
                    AS t(hotel_id, name, address, city, state, country, phone, email, website, latitude, longitude)
                ) v
                WHERE h.id = v.hotel_id
                """
                result = await conn.execute(
                    sql_hotels,
                    hotel_ids, names, addresses, cities, states, countries, phones, emails, websites, latitudes, longitudes
                )
                updated = int(result.split()[-1]) if result else 0
                
                # Update enrichment status for successful hotels
                success_urls = [u["booking_url"] for u in updates]
                success_statuses = [1] * len(success_urls)
                sql_status = """
                UPDATE sadie_gtm.hotel_booking_engines
                SET enrichment_status = v.status, last_enrichment_attempt = NOW()
                FROM (
                    SELECT * FROM unnest($1::text[], $2::int[]) AS t(booking_url, status)
                ) v
                WHERE hotel_booking_engines.booking_url = v.booking_url
                """
                await conn.execute(sql_status, success_urls, success_statuses)
            
            # Update enrichment status for failed hotels
            if failed_urls:
                failed_statuses = [-1] * len(failed_urls)
                sql_status = """
                UPDATE sadie_gtm.hotel_booking_engines
                SET enrichment_status = v.status, last_enrichment_attempt = NOW()
                FROM (
                    SELECT * FROM unnest($1::text[], $2::int[]) AS t(booking_url, status)
                ) v
                WHERE hotel_booking_engines.booking_url = v.booking_url
                """
                await conn.execute(sql_status, failed_urls, failed_statuses)
        
        return updated
