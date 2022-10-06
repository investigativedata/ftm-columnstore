import uuid

from followthemoney import model
from tests.util import ClickhouseTestCase

from ftm_columnstore import Dataset


class IOTestCase(ClickhouseTestCase):
    def test_io_import(self):
        # testdata already imported during tearUp
        count = self.driver.execute(
            f"SELECT count(*) FROM {self.driver.table} WHERE dataset = 'luanda_leaks'"
        )
        count = count[0][0]
        self.assertEqual(count, 4342)

    def test_io_import_with_fpx(self):
        # testdata already imported during tearUp
        count = self.driver.execute(
            f"SELECT count(*) FROM {self.driver.table} WHERE dataset = 'luanda_leaks_fpx'"
        )
        count = count[0][0]
        self.assertEqual(count, 4342)

    def test_io_import_numeric(self):
        def _test_numeric_value(value, result_value):
            dataset = Dataset("test_numeric")
            entity = model.make_entity("Payment")
            entity.add("amount", value)
            entity.make_id(str(uuid.uuid4()))
            res = dataset.put(entity)
            self.assertEqual(res, 1)
            entity = dataset.get(entity.id)
            amount = entity.get("amount")[0]
            self.assertEqual(value, amount)  # ftm props always strings
            res = dataset.driver.query(
                f"SELECT * FROM {dataset.driver.table} WHERE entity_id = '{entity.id}'"
            )
            values = list(res)[0]
            value_num = values[8]
            if result_value is not None:
                self.assertIsInstance(value_num, float)
                self.assertEqual(round(value_num, 1), result_value)
            else:
                self.assertEqual(value_num, result_value)

        _test_numeric_value("1000", 1000)
        _test_numeric_value("1000.0", 1000)
        _test_numeric_value("1000.1", 1000.1)
        _test_numeric_value("1000,1", 1000.1)
        _test_numeric_value("1,000,000", 1_000_000)
        _test_numeric_value("1,000,000.1", 1_000_000.1)
        _test_numeric_value("1.000.000", 1_000_000)
        _test_numeric_value("1.000.000,1", 1_000_000.1)
        _test_numeric_value("1.000.000.1", None)
        _test_numeric_value("1,000,000,1", None)
