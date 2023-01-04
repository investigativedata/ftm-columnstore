from tests.util import ClickhouseTestCase

from ftm_columnstore import get_dataset, search


class SearchTestCase(ClickhouseTestCase):
    def test_search_by_fingerprint(self):
        ds = get_dataset("luanda_leaks")
        q = "Isabel"
        res = search.search_entities(q, ds)
        res = [e.caption for e, score in res]
        self.assertListEqual(
            res,
            ["Isabel dos Santos", "Anisabel Moda e Acessorios, Limitada"],
        )
        res = search.search_entities(q, ds, limit=1)
        res = [e.caption for e, score in res]
        self.assertListEqual(res, ["Isabel dos Santos"])

        q = "Isabel dos"
        res = search.search_entities(q, ds)
        res = [e.caption for e, score in res]
        self.assertListEqual(res, ["Isabel dos Santos"])

        res = search.search_entities(q, ds, fuzzy=True)
        res = [e.caption for e, score in res]
        self.assertListEqual(res, ["Isabel dos Santos"])

    def test_search_empty(self):
        ds = get_dataset("luanda_leaks")
        q = "  "
        res = search.search_entities(q, ds)
        self.assertEqual(len(res), 0)

    def test_search_filters(self):
        ds = get_dataset("luanda_leaks")
        q = "Aero"
        query = ds.store.EQ.where(jurisdiction="gw")
        res = search.search_entities(q, ds, query)
        self.assertEqual(res[0][0].id, "01604606e019554f3c54f94f2b3537178b9a7046")
        query = query.where(schema="Person")
        res = search.search_entities(q, ds, query)
        self.assertEqual(len(res), 0)
