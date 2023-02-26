DELETE FROM `{{ params.project_id }}.silver.customers` WHERE TRUE
;

INSERT `{{ params.project_id }}.silver.customers` (
    client_id,
    first_name,
    last_name,
    email,
    registration_date,
    `state`
)
SELECT
    CAST(Id AS INTEGER) as client_id,
    FirstName as first_name,
    LastName as last_name,
    Email as email,
    CAST(RegistrationDate AS DATE) as registration_date,
    State as state,
FROM `{{ params.project_id }}.bronze.customers`
;
