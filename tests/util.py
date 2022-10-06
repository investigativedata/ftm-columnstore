import os
from unittest import TestCase

from ftm_columnstore import settings
from ftm_columnstore.io import import_json
from ftm_columnstore.driver import get_driver


def get_clickhouse_test_driver():
    settings.DATABASE_URI = "localhost"
    settings.DATABASE_TABLE = "ftm_columnstore_test"
    driver = get_driver()
    assert driver.table == "ftm_columnstore_test"
    return driver


class ClickhouseTestCase(TestCase):
    data_file = os.path.join(
        os.getcwd(), "tests", "fixtures", "icij_luanda_leaks.jsonl"
    )

    @classmethod
    def setUpClass(cls):
        cls.driver = get_clickhouse_test_driver()
        cls.driver.init(recreate=True)
        res1 = import_json(cls.data_file, "luanda_leaks")
        res2 = import_json(cls.data_file, "luanda_leaks_fpx", with_fingerprints=True)
        assert res1 == 852  # number of entities
        assert res1 == res2

    @classmethod
    def tearDownClass(cls):
        cls.driver.dangerous_drop()
