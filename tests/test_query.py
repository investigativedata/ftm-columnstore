from tests.util import ClickhouseTestCase

from ftm_columnstore.exceptions import InvalidQuery
from ftm_columnstore.query import Query, EntityQuery


class QueryTestCase(ClickhouseTestCase):
    maxDiff = None

    def test_query(self):
        q = Query()
        self.assertEqual(str(q), "SELECT * FROM ftm_columnstore_test")

        q = Query().select()
        self.assertEqual(str(q), "SELECT * FROM ftm_columnstore_test")

        q = Query("ftm_test")
        self.assertEqual(str(q), "SELECT * FROM ftm_test")

        # accept inner queries
        inner = Query().where(dataset="foo")
        q = Query(inner).select("count(distinct prop)")
        self.assertEqual(
            str(q),
            "SELECT count(distinct prop) FROM (SELECT * FROM ftm_columnstore_test WHERE dataset = 'foo')",
        )

        q = Query().select("entity_id", "prop")
        self.assertEqual(str(q), "SELECT entity_id, prop FROM ftm_columnstore_test")

        q = Query().select().where(country="de")
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE (prop = 'country' AND value = 'de')",
        )

        q = Query().select().where(country="de", date=2019)
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE (prop = 'country' AND value = 'de') OR (prop = 'date' AND value = '2019')",
        )

        q = Query().group_by("prop", "entity_id")
        self.assertEqual(
            str(q), "SELECT * FROM ftm_columnstore_test GROUP BY entity_id, prop"
        )

        q = Query().order_by("value")
        self.assertEqual(
            str(q), "SELECT * FROM ftm_columnstore_test ORDER BY value ASC"
        )

        q = Query().order_by("prop", "value", ascending=False)
        self.assertEqual(
            str(q), "SELECT * FROM ftm_columnstore_test ORDER BY prop, value DESC"
        )

    def test_query_where_operators(self):
        """
        .where(prop=value) turns into (prop = '$prop' AND value = '$value')
        or `value_num` for numeric properties based on ftm_columnstore_test schema
        except meta fields (dataset, entity_id, schema, origin)
        """
        q = Query().where(dataset="foo")
        self.assertEqual(
            str(q), "SELECT * FROM ftm_columnstore_test WHERE dataset = 'foo'"
        )

        q = Query().where(schema="Person")
        self.assertEqual(
            str(q), "SELECT * FROM ftm_columnstore_test WHERE schema = 'Person'"
        )

        q = Query().where(entity_id="foo")
        self.assertEqual(
            str(q), "SELECT * FROM ftm_columnstore_test WHERE entity_id = 'foo'"
        )

        q = Query().where(origin="foo")
        self.assertEqual(
            str(q), "SELECT * FROM ftm_columnstore_test WHERE origin = 'foo'"
        )

        q = Query().where(schema="Payment", amount__gt=0)
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE schema = 'Payment' AND (prop = 'amount' AND value_num > '0')",
        )

        # multiple ftm_columnstore_test prop lookups are combined with "OR" because of statement structure
        q = Query().where(name="foo", summary="bar")
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE (prop = 'name' AND value = 'foo') OR (prop = 'summary' AND value = 'bar')",
        )

        q = Query().where(name__like="nestle")
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE (prop = 'name' AND value LIKE 'nestle')",
        )

        q = Query().where(name__ilike="nestle")
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE (prop = 'name' AND value ILIKE 'nestle')",
        )

        q = Query().where(amount__gt=10)
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE (prop = 'amount' AND value_num > '10')",
        )

        q = Query().where(amount__gte=10)
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE (prop = 'amount' AND value_num >= '10')",
        )

        q = Query().where(amount__lt=10)
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE (prop = 'amount' AND value_num < '10')",
        )

        q = Query().where(amount__lte=10)
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE (prop = 'amount' AND value_num <= '10')",
        )

        q = Query().where(name__in=("alice", "lisa"))
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE (prop = 'name' AND value IN ('alice', 'lisa'))",
        )

        # IN will accept subqueries
        inner = Query().select("DISTINCT entity_id").where(name="alice")
        q = Query().where(entity_id__in=inner)
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE entity_id IN (SELECT DISTINCT entity_id FROM ftm_columnstore_test WHERE (prop = 'name' AND value = 'alice'))",
        )

    def test_query_slice(self):
        q = Query()[:100]
        self.assertEqual(str(q), "SELECT * FROM ftm_columnstore_test LIMIT 0, 100")

        q = Query()[100:200]
        self.assertEqual(str(q), "SELECT * FROM ftm_columnstore_test LIMIT 100, 100")

        q = Query()[100:]
        self.assertEqual(str(q), "SELECT * FROM ftm_columnstore_test OFFSET 100")

        q = Query()[17]
        self.assertEqual(str(q), "SELECT * FROM ftm_columnstore_test LIMIT 17, 1")

    def test_query_having(self):
        q = (
            Query()
            .select("entity_id", "sum(value_num) as amount_sum")
            .where(schema="Payment", amount__gt=0)
            .group_by("entity_id")
            .having(amount_sum__gte=100)
        )
        self.assertEqual(
            str(q),
            "SELECT entity_id, sum(value_num) as amount_sum FROM ftm_columnstore_test WHERE schema = 'Payment' AND (prop = 'amount' AND value_num > '0') GROUP BY entity_id HAVING amount_sum >= '100'",
        )

        # no having if no group by
        q = Query().having(foo="bar")
        self.assertEqual(str(q), "SELECT * FROM ftm_columnstore_test")
        self.assertEqual(
            str(q.group_by("foo")),
            "SELECT * FROM ftm_columnstore_test GROUP BY foo HAVING foo = 'bar'",
        )

    def test_query_correct_chain(self):
        q = (
            Query()
            .select("a")
            .where(name="bar")
            .select("b", "c")
            .where(amount=1, summary="f")
        )
        self.assertEqual(
            str(q),
            "SELECT a, b, c FROM ftm_columnstore_test WHERE (prop = 'amount' AND value_num = '1') OR (prop = 'name' AND value = 'bar') OR (prop = 'summary' AND value = 'f')",
        )

        # group by should be combined
        q = Query().group_by("a").group_by("b")
        self.assertEqual(str(q), "SELECT * FROM ftm_columnstore_test GROUP BY a, b")

        # order by should be overwritten!
        q = Query().order_by("a").order_by("b")
        self.assertEqual(str(q), "SELECT * FROM ftm_columnstore_test ORDER BY b ASC")

    def test_query_lookup_null(self):
        q = Query().where(name__null=True)
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE (prop = 'name' AND value IS NULL)",
        )
        q = Query().where(name__null=False)
        self.assertEqual(
            str(q),
            "SELECT * FROM ftm_columnstore_test WHERE (prop = 'name' AND value IS NOT NULL)",
        )

    def test_query_invalid(self):
        with self.assertRaisesRegex(InvalidQuery, "must not be negative"):
            q = Query()[-1]
            str(q)

        with self.assertRaisesRegex(InvalidQuery, "must not be negative"):
            q = Query()[100:50]
            str(q)

        with self.assertRaisesRegex(InvalidQuery, "steps not allowed"):
            q = Query()[100:50:2]
            str(q)

        with self.assertRaisesRegex(InvalidQuery, "Invalid operator"):
            q = Query().where(name__invalid_op=0)
            str(q)

        with self.assertRaisesRegex(InvalidQuery, "Invalid operator"):
            q = Query().where(name__invalid__op=0)
            str(q)

        # invalid ftm_columnstore_test props
        with self.assertRaisesRegex(InvalidQuery, "Invalid FtM property"):
            q = Query().where(invalid_prop=0)
            str(q)

        with self.assertRaisesRegex(InvalidQuery, "Invalid FtM property"):
            q = Query().where(invalid_prop__like=0)
            str(q)

    def test_entity_query(self):
        q = EntityQuery()
        self.assertEqual(
            str(q),
            "SELECT dataset, canonical_id, schema, groupArray(prop) as props, groupArray(values) as values FROM (SELECT dataset, canonical_id, schema, prop, groupUniqArray(value) as values FROM ftm_columnstore_test WHERE canonical_id IN (SELECT DISTINCT canonical_id FROM ftm_columnstore_test) GROUP BY canonical_id, dataset, prop, schema) GROUP BY canonical_id, dataset, schema ORDER BY dataset, canonical_id, schema ASC",
        )

        # all filters etc. will be applied to the innerst query
        q = EntityQuery().where(canonical_id=1)
        self.assertEqual(
            str(q),
            "SELECT dataset, canonical_id, schema, groupArray(prop) as props, groupArray(values) as values FROM (SELECT dataset, canonical_id, schema, prop, groupUniqArray(value) as values FROM ftm_columnstore_test WHERE canonical_id IN (SELECT DISTINCT canonical_id FROM ftm_columnstore_test WHERE canonical_id = '1') GROUP BY canonical_id, dataset, prop, schema) GROUP BY canonical_id, dataset, schema ORDER BY dataset, canonical_id, schema ASC",
        )
        q = EntityQuery().where(entity_id=1)
        self.assertEqual(
            str(q),
            "SELECT dataset, canonical_id, schema, groupArray(prop) as props, groupArray(values) as values FROM (SELECT dataset, canonical_id, schema, prop, groupUniqArray(value) as values FROM ftm_columnstore_test WHERE canonical_id IN (SELECT DISTINCT canonical_id FROM ftm_columnstore_test WHERE entity_id = '1') GROUP BY canonical_id, dataset, prop, schema) GROUP BY canonical_id, dataset, schema ORDER BY dataset, canonical_id, schema ASC",
        )

        q = EntityQuery()[:100]
        self.assertEqual(
            str(q),
            "SELECT dataset, canonical_id, schema, groupArray(prop) as props, groupArray(values) as values FROM (SELECT dataset, canonical_id, schema, prop, groupUniqArray(value) as values FROM ftm_columnstore_test WHERE canonical_id IN (SELECT DISTINCT canonical_id FROM ftm_columnstore_test LIMIT 0, 100) GROUP BY canonical_id, dataset, prop, schema) GROUP BY canonical_id, dataset, schema ORDER BY dataset, canonical_id, schema ASC",
        )

        # make sure dataset is passed along
        q = EntityQuery().where(dataset="luanda_leaks")
        self.assertEqual(
            str(q),
            "SELECT dataset, canonical_id, schema, groupArray(prop) as props, groupArray(values) as values FROM (SELECT dataset, canonical_id, schema, prop, groupUniqArray(value) as values FROM ftm_columnstore_test WHERE canonical_id IN (SELECT DISTINCT canonical_id FROM ftm_columnstore_test WHERE dataset = 'luanda_leaks') AND dataset = 'luanda_leaks' GROUP BY canonical_id, dataset, prop, schema) GROUP BY canonical_id, dataset, schema ORDER BY dataset, canonical_id, schema ASC",
        )
