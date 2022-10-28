from followthemoney import model
from followthemoney.util import make_entity_id
from tests.util import ClickhouseTestCase

from ftm_columnstore import get_dataset, statements


class CanonicalTestCase(ClickhouseTestCase):
    person = {"schema": "Person", "id": 1, "properties": {"name": ["Ann"]}}
    org = {
        "schema": "Organization",
        "id": 2,
        "properties": {"name": ["Follow The Money"]},
    }

    def test_canonical_statement(self):
        # by default, canonical id = entity id
        e = model.get_proxy(self.person)
        for stmt in statements.statements_from_entity(e, "test"):
            if stmt["prop"] == "id":
                self.assertEqual(stmt["value"], "1")
                self.assertEqual(stmt["entity_id"], "1")
                self.assertEqual(stmt["canonical_id"], "1")
                return

    def test_canonical_canonize(self):
        """
        canonize doesn't remove old statements, it only adds new ones with
        new canonical_id and adds statements to references
        and marks old statements as flag = 'canonized' to exclude from
        default queries
        """
        p = model.get_proxy(self.person)
        o = model.get_proxy(self.org)
        m = model.make_entity("Membership")
        m.add("member", p)
        m.add("organization", o)
        m.id = 3
        canonical_id = make_entity_id("C", p.id)
        ds = get_dataset("test_canonize")
        ds.put(p)
        ds.put(o)
        ds.put(m)
        self.assertEqual(ds.get_canonical_id("1"), "1")
        ds.canonize(p.id, canonical_id, sync=True)
        self.assertEqual(ds.get_canonical_id("1"), canonical_id)
        canonized = ds.get(canonical_id)
        self.assertEqual(canonized.id, canonical_id)
        self.assertEqual(canonized.get("name"), p.get("name"))
        self.assertEqual(ds.get_canonical_id(p.id), canonical_id)
        self.assertEqual(ds.get_canonical_id(canonical_id), canonical_id)

        # get connected membership entity with new canonized member id
        expanded = list(ds.expand(canonized))
        self.assertEqual(len(expanded), 1)
        new_m = expanded[0]
        self.assertEqual(canonized.id, new_m.first("member"))

        # get connected membership and organization
        expanded = list(ds.expand(canonized, levels=2))
        self.assertEqual(len(expanded), 2)

        # check we marked 5 statements as canonized
        old_stmts = ds.Q.where(sflag="canonized")
        self.assertEqual(len(old_stmts), 5)

        # org has only 1 membership pointing to it, the new canonized one
        org = ds.get("2")
        expanded = list(ds.expand(org))
        self.assertEqual(len(expanded), 1)
        # only 1 (merged) person
        q = ds.EQ.where(schema="Person")
        entities = list(q)
        # FIXME MergeTree dedupe not working correctly here
        self.assertEqual(len(entities), 2)
