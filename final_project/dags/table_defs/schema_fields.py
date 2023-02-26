SALES_BRONZE_SCHEMA_FIELDS = [
    {'name': 'CustomerId', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'Product', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'Price', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': '_id', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': '_logical_date', 'type': 'DATE', 'mode': 'REQUIRED'},
]

SALES_SILVER_SCHEMA_FIELDS = [
    {'name': 'client_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'purchase_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'product_name', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'price', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': '_id', 'type': 'STRING', 'mode': 'REQUIRED'},
]

USER_PROFILES_ENRICHED_SCHEMA = [
    {'name': 'client_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'registration_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'birth_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
]

CUSTOMERS_BRONZE_SCHEMA_FIELDS = [
    {'name': 'Id', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'Email', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'REQUIRED'},
    {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
]

CUSTOMERS_SILVER_SCHEMA_FIELDS = [
    {'name': 'client_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
    {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'registration_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
]
