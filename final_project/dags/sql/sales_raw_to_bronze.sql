DELETE FROM `{{ params.project_id }}.bronze.sales`
WHERE _logical_date = DATE('{{ds}}')
;

INSERT `{{ params.project_id }}.bronze.sales` (
    CustomerId,
    PurchaseDate,
    Product,
    Price,

    _id,
    _logical_date
)
SELECT
    CustomerId,
    PurchaseDate,
    Product,
    Price,

    GENERATE_UUID() AS _id,
    DATE('{{ds}}') as _logical_date
FROM sales_csv
;