from tests.util import ClickhouseTestCase

from ftm_columnstore.dataset import get_dataset


class DatasetTestCase(ClickhouseTestCase):
    def test_fingerprints(self):
        # find similarities by phonetic algorithm
        q = """SELECT entity_id FROM ftm_columnstore_test_fpx
            WHERE algorithm = '{algorithm}' AND value IN (
            SELECT value FROM (
                SELECT
                    count(DISTINCT entity_id) AS entities,
                    value
                FROM ftm_columnstore_test_fpx
                WHERE algorithm = '{algorithm}'
                GROUP BY value
                HAVING entities > 2
            ))"""
        ds = get_dataset("luanda_leaks")
        ds = ds.store
        entities = [
            e for e in ds.EQ.where(entity_id__in=q.format(algorithm="fingerprint"))
        ]
        self.assertEqual(len(entities), 125)
        # FIXME
        # names = [n for e in entities for n in e.names]
        # for name in [
        #     "Galp Energia Overseas Block 14 B.V.",
        #     "Galp Energia Overseas Block 32 B.V.",
        #     "Galp Energia Overseas Block 33 B.V.",
        #     "Windhoek PEL 23 B.V.",
        #     "Windhoek PEL 24 B.V.",
        #     "Windhoek PEL 28 B.V.",
        # ]:
        #     self.assertIn(name, names)
