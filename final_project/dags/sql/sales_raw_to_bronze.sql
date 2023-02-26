DELETE FROM `{{ params.project_id }}.bronze.sales`
WHERE _logical_date = DATE('{{ds}}')
;

INSERT `{{ params.project_id }}.bronze.sales` (
    CustomerId,
    PurchaseDate,
    Product,
    Price,

    _logical_date
)
SELECT
    CustomerId,
    PurchaseDate,
    Product,
    Price,

    DATE('{{ds}}') as _logical_date
FROM sales_csv
;