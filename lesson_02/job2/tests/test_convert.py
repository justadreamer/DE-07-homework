"""
Tests api.py
# TODO: write tests
"""
import json
import os
from unittest import TestCase
from lesson_02.job1.tests.common import remove_dir
from lesson_02.job2.convert import json_to_avro, convert_all_to_avro
from lesson_02.job2.convert import parsed_schema
import fastavro


class ConvertTestCase(TestCase):
    raw_dir = '/tmp/de07/tests/raw/sales/2022-12-18'
    stg_dir = '/tmp/de07/tests/stg/sales/2022-12-18'

    test_data = [
        {
            "client": "Kevin Mullen",
            "price": 998,
            "product": "Phone",
            "purchase_date": "2022-08-09"
        },
        {
            "client": "Elizabeth Brady",
            "price": 394,
            "product": "Microwave oven",
            "purchase_date": "2022-08-09"
        },
        {
            "client": "Kristen Lopez",
            "price": 1164,
            "product": "Laptop",
            "purchase_date": "2022-08-09"
        },
        {
            "client": "Ronald King",
            "price": 1957,
            "product": "coffee machine",
            "purchase_date": "2022-08-09"
        }
    ]

    def write_test_data(self, path):
        with open(path, 'wt') as f:
            json.dump(self.test_data, f)

    def setUp(self) -> None:
        os.makedirs(self.raw_dir, exist_ok=True)
        self.path1 = os.path.join(self.raw_dir, '2022-12-18.json')
        self.path2 = os.path.join(self.raw_dir, '2022-12-19.json')
        self.outpath1 = os.path.join(self.stg_dir, '2022-12-18.avro')
        self.outpath2 = os.path.join(self.stg_dir, '2022-12-19.avro')
        self.write_test_data(self.path1)
        self.write_test_data(self.path2)

    def tearDown(self) -> None:
        """
        Here we just delete all the temporary paths etc.
        """
        remove_dir(self.raw_dir)
        remove_dir(self.stg_dir)

    @staticmethod
    def read(path):
        with open(path, "rb") as f:
            records = list(fastavro.reader(f, parsed_schema))
        return records

    def testConvertOne(self):
        os.makedirs(self.stg_dir, exist_ok=True)
        json_to_avro(self.path1, self.outpath1)
        self.assertTrue(os.path.exists(self.outpath1))
        records = self.read(self.outpath1)
        self.assertEqual(records, self.test_data)

    def testConvertAll(self):
        convert_all_to_avro(self.raw_dir, self.stg_dir)
        self.assertTrue(os.path.exists(self.outpath1))
        self.assertTrue(os.path.exists(self.outpath2))
        records = self.read(self.outpath1)
        self.assertEqual(records, self.test_data)
        records = self.read(self.outpath2)
        self.assertEqual(records, self.test_data)

    def testOverwrite(self):
        old_test_data = [{
            "client": "Kevin Mullen",
            "price": 998,
            "product": "Phone",
            "purchase_date": "2022-08-09"
        }, {
            "client": "Elizabeth Brady",
            "price": 394,
            "product": "Microwave oven",
            "purchase_date": "2022-08-09"
        }]

        os.makedirs(self.stg_dir)
        with open(self.outpath1, 'wb+') as f:
            fastavro.writer(f, parsed_schema, old_test_data)
        records = self.read(self.outpath1)
        self.assertEqual(records, old_test_data)

        convert_all_to_avro(self.raw_dir, self.stg_dir)
        self.assertTrue(os.path.exists(self.outpath1))
        self.assertTrue(os.path.exists(self.outpath2))
        records = self.read(self.outpath1)
        self.assertEqual(records, self.test_data)
