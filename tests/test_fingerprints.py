from tests.util import ClickhouseTestCase

from ftm_columnstore.dataset import Dataset


class DatasetTestCase(ClickhouseTestCase):
    def test_fingerprints(self):
        # find similar soundex
        q = """SELECT entity_id FROM ftm_columnstore_test_fpx WHERE soundex IN (
            SELECT soundex FROM (
                SELECT
                    count(DISTINCT entity_id) AS entities,
                    soundex
                FROM ftm_columnstore_test_fpx
                GROUP BY soundex
                HAVING entities > 2
            ))"""
        ds = Dataset("luanda_leaks_fpx")
        entities = [e for e in ds.EQ.where(entity_id__in=q)]
        self.assertEqual(len(entities), 6)
        names = [e.caption for e in entities]
        self.assertSequenceEqual(
            sorted(names),
            [
                "Galp Energia Overseas Block 14 B.V.",
                "Galp Energia Overseas Block 32 B.V.",
                "Galp Energia Overseas Block 33 B.V.",
                "Windhoek PEL 23 B.V.",
                "Windhoek PEL 24 B.V.",
                "Windhoek PEL 28 B.V.",
            ],
        )
