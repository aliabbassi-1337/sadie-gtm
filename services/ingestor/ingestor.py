from pydantic.main import BaseModel

from services.ingestor import repo


class HotelPayload(BaseModel):
    name: str
    source: str
    status: str
    address: str
    city: str
    state: str
    phone: str
    country: str
    category: str

class Payload(BaseModel):
    hotelPayload: HotelPayload

class Ingestor:
    def __init__(self) -> None:
        pass

    async def ingest(self, payload: Payload):
        await self.ingest_hotel(payload.hotelPayload)

    async def ingest_hotel(self, hotelPayload: HotelPayload):
        await repo.insert_hotel(
            name=hotelPayload.name,
            address=hotelPayload.address,
            category=hotelPayload.category,
            city=hotelPayload.city,
            country=hotelPayload.country,
            phone=hotelPayload.phone,
            source=hotelPayload.source
        )
