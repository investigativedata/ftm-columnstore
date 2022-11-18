from tests.util import ClickhouseTestCase


class IOTestCase(ClickhouseTestCase):
    def test_io_import(self):
        # testdata already imported during tearUp
        count = self.driver.execute(
            f"SELECT count(*) FROM {self.driver.table} WHERE dataset = 'luanda_leaks'"
        )
        count = count[0][0]
        self.assertEqual(count, 5194)
        count = self.driver.execute(
            f"SELECT count(*) FROM {self.driver.table_fpx} WHERE dataset = 'luanda_leaks'"
        )
        count = count[0][0]
        self.assertEqual(count, 1873)
        count = self.driver.execute(
            f"SELECT count(*) FROM {self.driver.view_fpx_schemas}"
        )
        count = count[0][0]
        self.assertEqual(count, 1851)
