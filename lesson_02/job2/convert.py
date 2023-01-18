from fastavro import parse_schema, writer
import os
import json

schema = {
    'doc': 'Sales records',
    'name': 'Sales',
    'namespace': 'sales',
    'type': 'record',
    'fields': [
        {'name': 'client', 'type': 'string'},
        {'name': 'price', 'type': 'long'},
        {'name': 'product', 'type': 'string'},
        {'name': 'purchase_date', 'type': 'string'},
    ],
}

parsed_schema = parse_schema(schema)


def convert_all_to_avro(input_dir, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    for f in os.listdir(input_dir):
        name, ext = os.path.splitext(f)
        input_path = os.path.join(input_dir, f)
        output_path = os.path.join(output_dir, name+'.avro')
        json_to_avro(input_path, output_path)


def json_to_avro(input_path, output_path):
    if os.path.exists(output_path):
        os.remove(output_path)  # remove file if exists

    with open(output_path, 'wb+') as output:
        with open(input_path, 'rt') as _input:
            records = json.load(_input)
            writer(output, parsed_schema, records)
