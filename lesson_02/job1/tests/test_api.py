"""
Tests api.py
# TODO: write tests
"""
from unittest import TestCase, mock
from lesson_02.job1.api import get_sales
import requests
from lesson_02.job1.tests.common import TEST_TOKEN

class MockResponse:
    def __init__(self, json_data, status_code=200):
        self.json_data = json_data
        self.status_code = 200

    def json(self):
        return self.json_data

class GetSalesTestCase(TestCase):
    """
    Test api.get_sales function.
    """
    def testMultiPageResponse(self):
        def response(url, params, headers):
            if params['page'] < 3:
                return MockResponse([
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
                ])
            else:
                return MockResponse({"message": "No such page"})

        requests.get = mock.MagicMock(side_effect=response)
        result = get_sales('2022-12-15', auth_token=TEST_TOKEN)
        self.assertEqual(len(result), 6)

    def testNoneJsonResponse(self):
        requests.get = mock.MagicMock(return_value=MockResponse(None))
        result = get_sales('2022-12-15', auth_token=TEST_TOKEN)
        self.assertEqual(len(result), 0)

    def testEmptyJsonResponse(self):
        requests.get = mock.MagicMock(return_value=MockResponse([]))
        result = get_sales('2022-12-15', auth_token=TEST_TOKEN)
        self.assertEqual(len(result), 0)

    def test404Response(self):
        requests.get = mock.MagicMock(return_value=MockResponse([], status_code=404))
        result = get_sales('2022-12-15', auth_token=TEST_TOKEN)
        self.assertEqual(len(result), 0)

    def test500Response(self):
        requests.get = mock.MagicMock(return_value=MockResponse([], status_code=500))
        result = get_sales('2022-12-15', auth_token=TEST_TOKEN)
        self.assertEqual(len(result), 0)