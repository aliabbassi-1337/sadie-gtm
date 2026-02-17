-- Partial functional index for gov data matching.
-- Covers only the ~34k gov-sourced rows, indexed by lower(city) + lower(state)
-- so the find_gov_matches query can seek directly instead of scanning all gov rows.
CREATE INDEX CONCURRENTLY idx_hotels_gov_city_state
    ON sadie_gtm.hotels (lower(city), lower(state))
    WHERE source IN (
        'dbpr_license', 'dbpr_motel', 'texas_hot', 'sf_assessor',
        'la_county', 'md_sdat_cama', 'nyc_dof', 'hawaii_vpi',
        'chicago_license', 'nsw_liquor'
    );
