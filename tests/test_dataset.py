from tests.util import ClickhouseTestCase

from ftm_columnstore.dataset import DataCatalog, Dataset, get_dataset
from ftm_columnstore.exceptions import EntityNotFound
from ftm_columnstore.io import import_json


class DatasetTestCase(ClickhouseTestCase):
    def test_dataset_init(self):
        ds = Dataset.from_name("luanda_leaks")
        ds = ds.store
        self.assertEqual(ds.driver.table, "ftm_columnstore_test")
        self.assertEqual(ds.driver.table_fpx, "ftm_columnstore_test_fpx")
        self.assertIsNone(ds.origin)

        ds = get_dataset("luanda_leaks")
        self.assertIsInstance(ds, Dataset)
        ds = ds.store
        self.assertEqual(ds.driver.table, "ftm_columnstore_test")
        self.assertEqual(ds.driver.table_fpx, "ftm_columnstore_test_fpx")
        self.assertIsNone(ds.origin)

        # multiple datasets as catalog
        ds = DataCatalog.from_names(("foo", "bar"))
        self.assertIsInstance(ds, DataCatalog)
        ds = ds.store
        self.assertEqual(ds.driver.table, "ftm_columnstore_test")
        self.assertEqual(ds.driver.table_fpx, "ftm_columnstore_test_fpx")
        self.assertIsNone(ds.origin)

        ds = get_dataset("foo,bar")
        self.assertIsInstance(ds, DataCatalog)
        ds = ds.store
        self.assertEqual(ds.driver.table, "ftm_columnstore_test")
        self.assertEqual(ds.driver.table_fpx, "ftm_columnstore_test_fpx")
        self.assertIsNone(ds.origin)

    def test_dataset_queries(self):
        ds = get_dataset("foo")
        ds = ds.store
        self.assertEqual(
            str(ds.Q), "SELECT * FROM ftm_columnstore_test WHERE dataset IN ('foo')"
        )
        ds = get_dataset("foo,bar")
        ds = ds.store
        self.assertEqual(
            str(ds.Q),
            "SELECT * FROM ftm_columnstore_test WHERE dataset IN ('bar', 'foo')",
        )

    def test_dataset_iteration(self):
        import_json(self.data_file, "another_dataset")  # control group
        ds = get_dataset("luanda_leaks")
        ds = ds.store
        entities = [e for e in ds]
        self.assertEqual(len(entities), 852)
        entities = [e for e in ds.iterate(chunksize=500)]
        self.assertEqual(len(entities), 852)
        entities = [e for e in ds.iterate(limit=100, chunksize=500)]
        self.assertEqual(len(entities), 100)
        entities = [e for e in ds.iterate(limit=1000, chunksize=500)]
        self.assertEqual(len(entities), 852)
        entities = [e for e in ds.iterate(limit=100, chunksize=50)]
        self.assertEqual(len(entities), 100)
        entities = [e for e in ds.iterate(limit=101, chunksize=50)]
        self.assertEqual(len(entities), 101)
        entities = [e for e in ds.iterate(limit=101, chunksize=99)]
        self.assertEqual(len(entities), 101)

    def test_dataset_statements_iteration(self):
        ds = get_dataset("luanda_leaks")
        ds = ds.store
        stmts = [s for s in ds.statements()]
        self.assertEqual(len(stmts), 5194)

    def test_dataset_get_entity(self):
        ds = get_dataset("luanda_leaks")
        ds = ds.store
        entity = ds.get("0372a4b5d9c3f01f5b9eb1dddf8677ecc777b0a3")
        self.assertDictEqual(
            entity.to_dict(),
            {
                "datasets": ["luanda_leaks"],
                "id": "0372a4b5d9c3f01f5b9eb1dddf8677ecc777b0a3",
                "schema": "Company",
                "properties": {
                    "name": ["Sopor - Sociedade Distribuidora de Combustíveis, S.A."],
                    "dissolutionDate": ["2014-12-30"],
                    "alephUrl": [
                        "https://aleph.occrp.org/api/2/entities/0372a4b5d9c3f01f5b9eb1dddf8677ecc777b0a3.f14091b5d76ff7b3b2c846ba4ce68395ac36145c"
                    ],
                    "incorporationDate": ["1957-07-17"],
                    "jurisdiction": ["pt"],
                    "proof": ["6ea1f60f2634552390f3e9d1ac1b81a119d6d73e"],
                    "sector": ["Oil, Gas and Coal"],
                },
                "referents": [],
            },
        )

    def test_dataset_get_composite_entity(self):
        import_json(self.data_file, "luanda_leaks2")
        ds = get_dataset("luanda_leaks,luanda_leaks2")
        ds = ds.store
        entity = ds.get("0372a4b5d9c3f01f5b9eb1dddf8677ecc777b0a3")
        data = entity.to_dict()
        self.assertDictEqual(
            data["properties"],
            {
                "name": ["Sopor - Sociedade Distribuidora de Combustíveis, S.A."],
                "dissolutionDate": ["2014-12-30"],
                "alephUrl": [
                    "https://aleph.occrp.org/api/2/entities/0372a4b5d9c3f01f5b9eb1dddf8677ecc777b0a3.f14091b5d76ff7b3b2c846ba4ce68395ac36145c"
                ],
                "incorporationDate": ["1957-07-17"],
                "jurisdiction": ["pt"],
                "proof": ["6ea1f60f2634552390f3e9d1ac1b81a119d6d73e"],
                "sector": ["Oil, Gas and Coal"],
            },
        )
        self.assertSetEqual(
            set(entity.datasets), set(["luanda_leaks", "luanda_leaks2"])
        )

    def test_dataset_entity_resolve(self):
        ds = get_dataset("luanda_leaks")
        ds = ds.store
        entity = ds.get("003e86f3eae13d711ce9771f6bc3e47142069a7d")  # Ownership entity
        entities = [e for e in ds.resolve(entity)]
        self.assertEqual(2, len(entities))

    def test_dataset_entity_expand(self):
        ds = get_dataset("luanda_leaks")
        ds = ds.store
        isabel = ds.get("dfa01f04a87a4ce0f1e0f96a85d260a3249ab64a")
        entities = [e for e in ds.expand(isabel)]
        self.assertEqual(len(entities), 109)
        self.assertSetEqual(
            set([e.schema.name for e in entities]), set(["Ownership", "Table"])
        )

        # more levels
        entities = [e for e in ds.expand(isabel, levels=2)]
        self.assertEqual(len(entities), 218)

    def test_dataset_delete_entity(self):
        import_json(self.data_file, "luanda_leaks_to_delete")
        ds = get_dataset("luanda_leaks_to_delete")
        ds = ds.store
        entities = [e for e in ds]
        self.assertEqual(len(entities), 852)
        entity = ds.EQ.first()
        ds.delete(entity.id, sync=True)
        entities = [e for e in ds]
        self.assertEqual(len(entities), 851)
        self.assertRaises(EntityNotFound, lambda: ds.get(entity.id))

    def test_dataset_drop(self):
        import_json(self.data_file, "luanda_leaks_to_drop")
        ds = get_dataset("luanda_leaks_to_drop")
        ds = ds.store
        entities = [e for e in ds]
        self.assertEqual(len(entities), 852)
        ds.drop(sync=True)
        entities = [e for e in ds]
        self.assertEqual(len(entities), 0)
