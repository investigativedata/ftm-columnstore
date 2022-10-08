from followthemoney import model
from tests.util import ClickhouseTestCase

from ftm_columnstore import statements, get_dataset


class CanonicalTestCase(ClickhouseTestCase):
    def test_canonical_statement(self):
        # ids are always strings and canonical_ids always sha1 digest
        e = model.make_entity("Person")
        e.id = 1
        e.add("name", "Simon")
        for stmt in statements.statements_from_entity(e, "test"):
            if stmt["prop"] == "id":
                self.assertEqual(stmt["value"], "1")
                self.assertEqual(stmt["entity_id"], "1")
                self.assertEqual(
                    stmt["canonical_id"], "356a192b7913b04c54574d18c28d46e6395428ab"
                )
                return

    def test_canonical_dataset_get(self):
        ds = get_dataset("test")
        e = model.make_entity("Person")
        e.id = 1
        e.add("name", "Simon")
        ds.put(e)
        entity = ds.get("1", canonical=False)
        self.assertEqual(entity.get("name"), e.get("name"))
        self.assertEqual(entity.id, "356a192b7913b04c54574d18c28d46e6395428ab")
        entity = ds.get("356a192b7913b04c54574d18c28d46e6395428ab")
        self.assertEqual(entity.id, "356a192b7913b04c54574d18c28d46e6395428ab")
        self.assertEqual(e.get("name"), entity.get("name"))
