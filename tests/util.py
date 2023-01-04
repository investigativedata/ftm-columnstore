import os
from unittest import TestCase

from ftm_columnstore import settings
from ftm_columnstore.driver import get_driver
from ftm_columnstore.io import import_json


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
        res = import_json(cls.data_file, "luanda_leaks")
        assert res == 852  # number of entities
        cls.driver.optimize(full=True)

    @classmethod
    def tearDownClass(cls):
        cls.driver.dangerous_drop()
