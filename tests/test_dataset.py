from tests.util import ClickhouseTestCase

from ftm_columnstore.dataset import Dataset


class DatasetTestCase(ClickhouseTestCase):
    def test_dataset_init(self):
        ds = Dataset("luanda_leaks")
        self.assertEqual(ds.driver.table, "ftm_columnstore_test")
        self.assertEqual(ds.driver.table_fpx, "ftm_columnstore_test_fpx")
        self.assertIsNone(ds.origin)

    def test_dataset_iteration(self):
        ds = Dataset("luanda_leaks")
        entities = [e for e in ds]
        self.assertEqual(len(entities), 852)
        entities = [e for e in ds.iterate(chunksize=500)]
        self.assertEqual(len(entities), 852)

    def test_dataset_statements_iteration(self):
        ds = Dataset("luanda_leaks")
        stmts = [s for s in ds.statements()]
        self.assertEqual(len(stmts), 5194)

    def test_dataset_get_entity(self):
        ds = Dataset("luanda_leaks")
        entity = ds.get("0372a4b5d9c3f01f5b9eb1dddf8677ecc777b0a3")
        self.assertDictEqual(
            entity.to_dict(),
            {
                "id": "0372a4b5d9c3f01f5b9eb1dddf8677ecc777b0a3",
                "schema": "Company",
                "properties": {
                    "alephUrl": [
                        "https://aleph.occrp.org/api/2/entities/0372a4b5d9c3f01f5b9eb1dddf8677ecc777b0a3.f14091b5d76ff7b3b2c846ba4ce68395ac36145c"
                    ],
                    "sector": ["Oil, Gas and Coal"],
                    "dissolutionDate": ["2014-12-30"],
                    "name": ["Sopor - Sociedade Distribuidora de Combustíveis, S.A."],
                    "incorporationDate": ["1957-07-17"],
                    "proof": ["6ea1f60f2634552390f3e9d1ac1b81a119d6d73e"],
                    "jurisdiction": ["pt"],
                },
            },
        )

    def test_dataset_entity_resolve(self):
        ds = Dataset("luanda_leaks")
        entity = ds.get("003e86f3eae13d711ce9771f6bc3e47142069a7d")  # Ownership entity
        entities = [e for e in ds.resolve(entity)]
        self.assertEqual(2, len(entities))

    def test_dataset_entity_expand(self):
        ds = Dataset("luanda_leaks")
        isabel = ds.get("dfa01f04a87a4ce0f1e0f96a85d260a3249ab64a")
        entities = [e for e in ds.expand(isabel)]
        self.assertEqual(len(entities), 109)
        self.assertSetEqual(
            set([e.schema.name for e in entities]), set(["Ownership", "Table"])
        )

        # more levels
        entities = [e for e in ds.expand(isabel, levels=2)]
        self.assertEqual(len(entities), 219)
