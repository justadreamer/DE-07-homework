DELETE FROM `{{ params.project_id }}.bronze.sales`
WHERE _logical_date = DATE('{{ds}}')
;

INSERT `{{ params.project_id }}.bronze.sales` (
    customer_id,
    purchase_date,
    product,
    price,

    _logical_date
)
SELECT
    customer_id,
    purchase_date,
    product,
    price,

    DATE('{{ds}}') as _logical_date
FROM sales_csv
;