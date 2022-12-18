"""
Tests for main.py
# TODO: write tests
"""
import os
from unittest import TestCase, mock

import lesson_02.job1.tests.common
from lesson_02.job1.tests.common import TEST_TOKEN
from lesson_02.job1 import main


class MainFunctionTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()


    @mock.patch('lesson_02.job1.api.get_sales')
    def test_return_400_date_param_missed_path_param_missed(
            self,
            get_sales_mock: mock.MagicMock
        ):
        """
        Raise 400 HTTP code when no 'date' param
        """
        resp = self.client.post(
            '/',
            json={
                'raw_dir': '/foo/bar/',
                # no 'date' set!
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_return_400_raw_dir_param_missed(self):
        resp = self.client.post(
            '/',
            json = {
                'date': '2022-08-09'
            },
        )

        self.assertEqual(400, resp.status_code)

    @mock.patch('lesson_02.job1.storage.save_to_disk')
    @mock.patch('lesson_02.job1.api.get_sales')
    def test_api_get_sales_called(
            self,
            get_sales_mock: mock.MagicMock,
            save_to_disk_mock: mock.MagicMock
    ):
        """
        Test whether api.get_sales is called with proper params
        """
        fake_date = '1970-01-01'
        self.client.post(
            '/',
            json={
                'date': fake_date,
                'raw_dir': '/foo/bar/',
            },
        )

        get_sales_mock.assert_called_with(date=fake_date, auth_token=TEST_TOKEN)
        save_to_disk_mock.assert_called()

    @mock.patch('lesson_02.job1.storage.save_to_disk')
    @mock.patch('lesson_02.job1.api.get_sales')
    def test_return_201_when_all_is_ok(
            self,
            get_sales_mock: mock.MagicMock,
            save_to_disk_mock: mock.MagicMock,
    ):
        test_data = [{
            "json_key": "json_value"
        }]
        get_sales_mock.return_value = test_data
        fake_date = '1970-01-01'
        fake_dir = '/foo/bar/'
        self.client.post(
            '/',
            json={
                'date': fake_date,
                'raw_dir': fake_dir,
            },
        )

        get_sales_mock.assert_called_with(date=fake_date, auth_token=TEST_TOKEN)
        save_to_disk_mock.assert_called_with(test_data, os.path.join(fake_dir, fake_date + '.json'))

    @mock.patch('lesson_02.job1.storage.save_to_disk')
    @mock.patch('lesson_02.job1.api.get_sales')
    def test_return_201_even_for_empty_data(
            self,
            get_sales_mock: mock.MagicMock,
            save_to_disk_mock: mock.MagicMock,
    ):
        empty_data = []
        get_sales_mock.return_value = empty_data
        fake_date = 'never'
        fake_dir = '/foo/bar/'
        resp = self.client.post(
            '/',
            json={
                'date': fake_date,
                'raw_dir': fake_dir,
            },
        )
        get_sales_mock.assert_called_with(date=fake_date, auth_token=TEST_TOKEN)
        save_to_disk_mock.assert_called_with(empty_data, os.path.join(fake_dir, fake_date + '.json'))
        self.assertEqual(resp.status_code, 201)
