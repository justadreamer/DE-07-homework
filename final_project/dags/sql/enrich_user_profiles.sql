DELETE FROM `{{ params.project_id }}.gold.user_profiles_enriched`
WHERE TRUE
;

INSERT `{{ params.project_id }}.gold.user_profiles_enriched` (
        client_id,
        first_name,
        last_name,
        email,
        phone_number,
        registration_date,
        birth_date,
        state
)
SELECT
    c.client_id as client_id,
    CASE
        WHEN c.first_name IS NULL AND up.full_name IS NOT NULL THEN SPLIT(up.full_name, ' ')[ORDINAL(1)]
        ELSE c.first_name
    END as first_name,
    CASE
        WHEN c.last_name IS NULL AND up.full_name IS NOT NULL THEN SPLIT(up.full_name, ' ')[ORDINAL(2)]
        ELSE c.last_name
    END as last_name,
    c.email,
    up.phone_number,
    c.registration_date,
    up.birth_date,
    CASE
        WHEN c.state IS NULL AND up.state IS NOT NULL THEN up.state
        ELSE c.state
    END as state
FROM `{{ params.project_id }}.silver.customers` AS c
LEFT JOIN `{{ params.project_id }}.silver.user_profiles` AS up
ON c.email = up.email
;