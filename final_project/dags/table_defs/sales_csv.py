sales_csv = {
    "autodetect": False,
    "schema": {
        "fields": [
            {
                "name": "customer_id",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "purchase_date",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "product",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "price",
                "type": "STRING",
                "mode": "NULLABLE"
            },
        ]
    },
    "csvOptions": {
        "allowJaggedRows": False,
        "allowQuotedNewlines": False,
        "maxBadRecords": 0,
        "encoding": "UTF-8",
        "quote": "\"",
        "fieldDelimiter": ",",
        "skipLeadingRows": 1
    },
    "sourceFormat": "CSV",
    "sourceUris": [
        (
            "gs://{{ params.raw_bucket }}"
            "/sales"
            "/{{ dag_run.logical_date.strftime('%Y-%m-%-d') }}"
            "/*.csv"
        )
    ]
}
