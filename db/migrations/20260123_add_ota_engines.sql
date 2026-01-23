-- Add OTAs as booking engines with tier=0
-- These can be filtered out in reporting but tracked for detection stats

INSERT INTO sadie_gtm.booking_engines (name, domains, tier, is_active) VALUES
    ('Booking.com', ARRAY['booking.com'], 0, true),
    ('Expedia', ARRAY['expedia.com'], 0, true),
    ('Hotels.com', ARRAY['hotels.com'], 0, true),
    ('Airbnb', ARRAY['airbnb.com'], 0, true),
    ('VRBO', ARRAY['vrbo.com'], 0, true),
    ('Agoda', ARRAY['agoda.com'], 0, true),
    ('Priceline', ARRAY['priceline.com'], 0, true),
    ('Hotwire', ARRAY['hotwire.com'], 0, true),
    ('Hostelworld', ARRAY['hostelworld.com'], 0, true),
    ('Trivago', ARRAY['trivago.com'], 0, true),
    ('Kayak', ARRAY['kayak.com'], 0, true)
ON CONFLICT (name) DO UPDATE SET tier = 0;
