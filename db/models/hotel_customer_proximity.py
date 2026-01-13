from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, ConfigDict


class HotelCustomerProximity(BaseModel):
    """Hotel customer proximity model matching the database schema."""

    id: int
    hotel_id: int
    existing_customer_id: int
    distance_km: Decimal
    computed_at: datetime

    model_config = ConfigDict(from_attributes=True)
