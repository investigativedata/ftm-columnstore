from tests.util import ClickhouseTestCase

from ftm_columnstore import get_dataset, nk, xref


class XrefTestCase(ClickhouseTestCase):
    maxDiff = None

    def test_xref(self):
        # xref 1 dataset against itself
        ds = get_dataset("luanda_leaks")
        loader = nk.ClickhouseLoader([ds])
        query = loader.get_query()
        self.assertEqual(
            str(query),
            "SELECT metaphone1, groupUniqArray(dataset) AS datasets, groupUniqArray(entity_id) AS ids FROM ftm_columnstore_test_fpx WHERE dataset IN ('luanda_leaks') AND metaphone1 <> '' AND metaphone1 IS NOT NULL AND prop = 'name' GROUP BY metaphone1 HAVING length(datasets) > '0' AND length(ids) > '1'",
        )
        blocks = [x for x in query]
        self.assertEqual(len(blocks), 3)
        chunk = next(loader.get_chunks(left_dataset=ds))
        self.assertEqual(len([e for e in chunk]), 2)

        result = next(xref.xref_dataset(ds))
        self.assertEqual(len(result.resolver.edges), 1)

        # xref 1 dataset against 1 or more others
        # FIXME need more datasets to test, here we only check
        # if the query is correctly built
        left_dataset = get_dataset("luanda_leaks")
        ds = get_dataset("empty_dataset")
        loader = nk.ClickhouseLoader([left_dataset, ds], left_dataset)
        query = loader.get_query()
        self.assertEqual(
            str(query),
            "SELECT metaphone1, groupUniqArray(dataset) AS datasets, groupUniqArray(entity_id) AS ids FROM ftm_columnstore_test_fpx WHERE dataset IN ('luanda_leaks', 'empty_dataset') AND metaphone1 <> '' AND metaphone1 IN (SELECT DISTINCT metaphone1 FROM ftm_columnstore_test_fpx WHERE dataset = 'luanda_leaks') AND metaphone1 IS NOT NULL AND prop = 'name' GROUP BY metaphone1 HAVING length(datasets) > '1' AND length(ids) > '1'",
        )
        self.assertRaises(
            StopIteration,
            lambda: next(xref.xref_datasets([left_dataset, ds], left_dataset)),
        )

        # xref datasets against each other
        ds2 = get_dataset("empty_dataset2")
        loader = nk.ClickhouseLoader([left_dataset, ds, ds2])
        query = loader.get_query()
        self.assertEqual(
            str(query),
            "SELECT metaphone1, groupUniqArray(dataset) AS datasets, groupUniqArray(entity_id) AS ids FROM ftm_columnstore_test_fpx WHERE dataset IN ('luanda_leaks', 'empty_dataset', 'empty_dataset2') AND metaphone1 <> '' AND metaphone1 IS NOT NULL AND prop = 'name' GROUP BY metaphone1 HAVING length(datasets) > '1' AND length(ids) > '1'",
        )
        self.assertRaises(
            StopIteration, lambda: next(xref.xref_datasets([left_dataset, ds, ds2]))
        )
