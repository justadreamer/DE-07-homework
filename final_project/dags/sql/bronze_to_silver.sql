DELETE FROM `{{ params.project_id }}.silver.sales`
WHERE purchase_date = DATE('{{ds}}')
;

INSERT `{{ params.project_id }}.silver.sales` (
    customer_id,
    purchase_date,
    product,
    price
)
SELECT
    customer_id,
    _logical_date as purchase_date,
    product,
    CAST(RTRIM(RTRIM(price, 'USD'), '$') AS INTEGER) AS price
FROM `{{ params.project_id }}.bronze.sales`
WHERE _logical_date = DATE('{{ds}}')
;