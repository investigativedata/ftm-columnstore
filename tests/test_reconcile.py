from followthemoney import model
from tests.util import ClickhouseTestCase

from ftm_columnstore import get_dataset, reconcile


class ReconcileTestcase(ClickhouseTestCase):
    def test_reconcile_schema(self):
        ds = get_dataset("luanda_leaks")
        entity = ds.EQ.where(schema="Company").first()
        ds = get_dataset("other")
        entity.id = 1
        ds.put(entity)
        entity.id = 2
        data = entity.to_dict()
        data["schema"] = "Organization"
        ds.put(model.get_proxy(data))
        entity = model.get_proxy(
            {
                "id": 3,
                "schema": "LegalEntity",
                "properties": {"name": ["Wise Intelligence Solutions Holding Limited"]},
            }
        )
        schema, score = reconcile.guess_schema(entity)
        self.assertEqual(schema, "Company")
        self.assertEqual(score, 2 / 3)
