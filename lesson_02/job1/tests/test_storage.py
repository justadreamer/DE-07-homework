"""
Tests api.py
# TODO: write tests
"""
import json
import os.path
from unittest import TestCase, mock

from lesson_02.job1.storage import save_to_disk

class SaveToDiskTestCase(TestCase):
    temp_dir = '/tmp/de07/tests/raw/sales'
    test_data = [{
                "key1": "value1",
                "key2": "value2",
                "key3": "value3"
            },
            {
                "key4": "value4",
                "key5": "value5",
                "key6": "value6"
            }]
    """
    Test storage.save_to_disk function.
    """
    def tearDown(self) -> None:
        """
        Here we just delete all the temporary paths etc.
        """
        for f in os.listdir(self.temp_dir):
            os.remove(os.path.join(self.temp_dir, f))
        os.rmdir(self.temp_dir)

    def read(self, path):
        data = None
        with open(path, 'rt+') as f:
            data = json.load(f)
        return data

    def testWriteCorrectData(self):
        #verify writing to the path
        path = os.path.join(self.temp_dir, 'test.json')
        save_to_disk(self.test_data, path)
        readData = self.read(path)
        self.assertIsNotNone(readData)
        self.assertEqual(readData, self.test_data)

    def testOverwriteData(self):
        """
        We precreate a file with some data and verify that it is overwritten with the new data
        """
        old_data = [{
                "old_key1": "value1",
                "old_key2": "value2",
                "old_key3": "value3"
            }]
        path = os.path.join(self.temp_dir, 'test.json')
        os.makedirs(self.temp_dir)
        with open(path, 'wt+') as f:
            json.dump(old_data, f)

        self.assertEqual(old_data, self.read(path))

        save_to_disk(self.test_data, path)
        readData = self.read(path)
        self.assertNotEqual(old_data, readData)
        self.assertEqual(self.test_data, readData)
