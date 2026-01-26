-- Enable pg_trgm extension for fuzzy text matching
-- This allows similarity-based hotel name deduplication
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Add GIN index for trigram similarity on hotel names
CREATE INDEX IF NOT EXISTS idx_hotels_name_trgm 
ON sadie_gtm.hotels USING GIN (name gin_trgm_ops);

-- Function to find similar hotels by name with similarity threshold
CREATE OR REPLACE FUNCTION sadie_gtm.find_similar_hotel(
    p_name TEXT,
    p_city TEXT DEFAULT NULL,
    p_threshold FLOAT DEFAULT 0.6
)
RETURNS TABLE (
    id INT,
    name TEXT,
    city TEXT,
    source TEXT,
    similarity FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        h.id,
        h.name,
        h.city,
        h.source,
        similarity(h.name, p_name) AS similarity
    FROM sadie_gtm.hotels h
    WHERE similarity(h.name, p_name) > p_threshold
      AND (p_city IS NULL OR h.city IS NULL OR LOWER(h.city) = LOWER(p_city))
    ORDER BY similarity DESC
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Example usage:
-- SELECT * FROM sadie_gtm.find_similar_hotel('The Grand Hotel', 'Miami', 0.7);
-- Returns matching hotel if "Grand Hotel Miami" exists with similarity > 0.7
