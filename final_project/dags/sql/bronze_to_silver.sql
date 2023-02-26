DELETE FROM `{{ params.project_id }}.silver.sales`
WHERE purchase_date = DATE('{{ds}}')
;

INSERT `{{ params.project_id }}.silver.sales` (
    client_id,
    purchase_date,
    product_name,
    price
)
SELECT
    CustomerId as client_id,
    _logical_date as purchase_date,
    Product as product_name,
    CAST(RTRIM(RTRIM(Price, 'USD'), '$') AS INTEGER) AS price
FROM `{{ params.project_id }}.bronze.sales`
WHERE _logical_date = DATE('{{ds}}')
;