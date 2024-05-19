def test_fingerprints():
    # find similarities by phonetic algorithm
    # FIXME query here for reference
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
