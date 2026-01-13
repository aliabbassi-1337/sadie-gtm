-- Seed data for local development and testing
-- This file is automatically loaded when starting the Docker database

SET search_path TO sadie_gtm;

-- Insert sample target markets
INSERT INTO target_markets (city, state, location, max_radius_km) VALUES
    ('Miami', 'Florida', ST_Point(-80.1918, 25.7617)::geography, 25),
    ('Orlando', 'Florida', ST_Point(-81.3792, 28.5383)::geography, 30),
    ('Key West', 'Florida', ST_Point(-81.7800, 24.5551)::geography, 10)
ON CONFLICT (city, state, country) DO NOTHING;

-- Insert sample hotels
INSERT INTO hotels (name, website, location, city, state, country, location_status, status, source) VALUES
    (
        'Test Hotel Miami Beach',
        'https://testhotel.com',
        ST_Point(-80.1300, 25.7907)::geography,
        'Miami Beach',
        'Florida',
        'USA',
        0,  -- pending validation
        0,  -- scraped
        'test'
    ),
    (
        'Sample Orlando Resort',
        'https://sampleresort.com',
        ST_Point(-81.3792, 28.5383)::geography,
        'Orlando',
        'Florida',
        'USA',
        1,  -- validated
        0,  -- scraped
        'test'
    )
ON CONFLICT (name, COALESCE(website, '')) DO NOTHING;
