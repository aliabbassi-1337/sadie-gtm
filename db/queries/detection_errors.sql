-- name: insert_detection_error!
-- Log a detection error for debugging
INSERT INTO detection_errors (hotel_id, error_type, error_message, detected_location)
VALUES (:hotel_id, :error_type, :error_message, :detected_location);

-- name: get_detection_errors_by_type
-- Get detection errors by type for analysis
SELECT id, hotel_id, error_type, error_message, detected_location, created_at
FROM detection_errors
WHERE error_type = :error_type
LIMIT :limit;

-- name: get_detection_errors_summary
-- Get count of errors by type
SELECT error_type, COUNT(*) as count
FROM detection_errors
GROUP BY error_type;
