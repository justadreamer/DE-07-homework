"""
Tests for main.py
# TODO: write tests
"""
import os
from unittest import TestCase, mock

from lesson_02.job2 import main


class MainFunctionTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        main.app.testing = True
        cls.client = main.app.test_client()

    def test_return_400_raw_dir_param_missed(self):
        resp = self.client.post(
            '/',
            json = {
                'stg_dir': '/foo/bar/'
            },
        )

        self.assertEqual(400, resp.status_code)

    def test_return_400_stg_dir_param_missed(self):
        resp = self.client.post(
            '/',
            json={
                'raw_dir': '/foo/bar/'
            },
        )

        self.assertEqual(400, resp.status_code)



    @mock.patch('lesson_02.job2.convert.convert_all_to_avro')
    def test_return_201_when_all_is_ok(
            self,
            convert_all_to_avro_mock: mock.MagicMock,
    ):
        raw_dir = '/foo/bar/raw'
        stg_dir = '/foo/bar/stg'
        resp = self.client.post(
            '/',
            json={
                'raw_dir': raw_dir,
                'stg_dir': stg_dir,
            },
        )
        self.assertEqual(resp.status_code, 201)
        convert_all_to_avro_mock.assert_called_with(raw_dir, stg_dir)
