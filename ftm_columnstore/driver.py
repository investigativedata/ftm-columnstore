from functools import lru_cache
from typing import Any, Iterable, Iterator, Optional

import pandas as pd
from clickhouse_driver import Client, errors

from . import settings


def table_exists(e: Exception, table: str) -> bool:
    if f"Table default.{table} already exists" in str(e):
        return True
    if "projection with this name already exists" in str(e):
        return True
    return False


class ClickhouseDriver:
    def __init__(
        self,
        uri: Optional[str] = settings.DATABASE_URI,
        table: Optional[str] = settings.DATABASE_TABLE,
    ):
        self.table = table
        self.table_fpx = f"{table}_fpx"
        self.uri = uri
        self.ensure_table()

    def __str__(self):
        return self.uri

    def __repr__(self):
        return f"<{self.__class__.__name__} ({self})>"

    def init(self, recreate: Optional[bool] = False, exists_ok: Optional[bool] = False):
        if recreate:
            self.dangerous_drop()
        for stmt in self.create_statements:
            try:
                self.execute(stmt)
            except Exception as e:
                if exists_ok and (
                    table_exists(e, self.table) or table_exists(e, self.table_fpx)
                ):
                    pass
                else:
                    raise e

    def dangerous_drop(self):
        for stmt in self.drop_statements:
            self.execute(stmt)

    def ensure_table(self):
        try:
            self.init(recreate=False)
        except errors.ServerException as e:
            if table_exists(e, self.table) or table_exists(e, self.table_fpx):
                pass
            else:
                raise e

    def insert(self, df: pd.DataFrame, table: Optional[str] = None) -> int:
        # https://clickhouse-driver.readthedocs.io/en/latest/features.html#numpy-pandas-support
        if df.empty:
            return 0
        table = table or self.table
        with self.get_connection() as conn:
            res = conn.insert_dataframe("INSERT INTO %s VALUES" % table, df)
        return res

    def query(self, query: Any, *args, **kwargs) -> Iterator[Any]:
        query = str(query)
        conn = self.get_connection()
        kwargs = {**{"settings": {"max_block_size": 100000}}, **kwargs}
        return conn.execute_iter(query, *args, **kwargs)
        # with self.get_connection() as conn:
        #     res = conn.execute_iter(query, settings={"max_block_size": 100000})
        # return res

    def query_dataframe(self, query: Any) -> pd.DataFrame:
        query = str(query)
        with self.get_connection() as conn:
            return conn.query_dataframe(query)

    def get_connection(
        self,
        uri: Optional[str] = settings.DATABASE_URI,
    ):
        host, *port = uri.split(":")
        if not port:
            port = [9000]
        return Client(settings={"use_numpy": True}, host=host, port=port[0])

    def execute(self, *args, **kwargs):
        with self.get_connection() as conn:
            res = conn.execute(*args, **kwargs)
        return res

    def execute_iter(self, *args, **kwargs):
        return self.query(*args, **kwargs)

    @property
    def create_statements(self) -> Iterable[str]:
        create_table = f"""
        CREATE TABLE {self.table}
        (
            `id`                      FixedString(40),
            `dataset`                 LowCardinality(String),
            `canonical_id`            String,
            `entity_id`               String,
            `schema`                  LowCardinality(String),
            `origin`                  LowCardinality(String),
            `prop`                    LowCardinality(String),
            `prop_type`               LowCardinality(String),
            `value`                   String,
            `ts`                      DateTime64,
            `sflag`                   LowCardinality(String)
        ) ENGINE = ReplacingMergeTree(ts)
        PRIMARY KEY (dataset,schema,canonical_id)
        ORDER BY (dataset,schema,canonical_id,entity_id,origin,prop,value)
        """

        create_table_fpx = f"""
        CREATE TABLE {self.table_fpx}
        (
            `id`                      FixedString(40),
            `dataset`                 LowCardinality(String),
            `entity_id`               String,
            `schema`                  LowCardinality(String),
            `prop`                    LowCardinality(String),
            `fingerprint`             String,
            `fingerprint_id`          FixedString(40),
            `soundex`                 String,
            `metaphone1`              String,
            `metaphone2`              String NULL,
            INDEX fp_ix (fingerprint) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
        ) ENGINE = ReplacingMergeTree()
        PRIMARY KEY (fingerprint_id,schema,dataset)
        ORDER BY (fingerprint_id,schema,dataset)
        """

        projections = (
            f"""ALTER TABLE {self.table} ADD PROJECTION {self.table}_values (
                SELECT * ORDER BY value,prop)""",
            f"""ALTER TABLE {self.table} ADD PROJECTION {self.table}_canonical_lookup (
                SELECT * ORDER BY entity_id,canonical_id)""",
            f"""ALTER TABLE {self.table} ADD PROJECTION {self.table}_canonical_id (
                SELECT * ORDER BY canonical_id,prop)""",
            f"""ALTER TABLE {self.table} ADD PROJECTION {self.table}_entity_id (
                SELECT * ORDER BY entity_id,prop)""",
            f"""ALTER TABLE {self.table} ADD PROJECTION {self.table}_prop_type (
                SELECT * ORDER BY prop_type,schema,dataset)""",
            f"""ALTER TABLE {self.table} ADD PROJECTION {self.table}_prop (
                SELECT * ORDER BY prop,schema,dataset)""",
            f"""ALTER TABLE {self.table} ADD PROJECTION {self.table}_entities (
                SELECT dataset,canonical_id,schema,prop,groupUniqArray(value) as values
                GROUP BY dataset,canonical_id,schema,prop)""",
            f"""ALTER TABLE {self.table_fpx} ADD PROJECTION {self.table_fpx}_soundex (
                SELECT * ORDER BY soundex,schema,dataset)""",
            f"""ALTER TABLE {self.table_fpx} ADD PROJECTION {self.table_fpx}_metaphone1 (
                SELECT * ORDER BY metaphone1,schema,dataset)""",
            f"""ALTER TABLE {self.table_fpx} ADD PROJECTION {self.table_fpx}_metaphone2 (
                SELECT * ORDER BY metaphone2,schema,dataset)""",
        )
        return (create_table, create_table_fpx, *projections)

    @property
    def drop_statements(self) -> str:
        return (
            f"DROP TABLE IF EXISTS {self.table}",
            f"DROP TABLE IF EXISTS {self.table_fpx}",
        )


@lru_cache(128)
def get_driver(
    uri: Optional[str] = None,
    table: Optional[str] = None,
) -> ClickhouseDriver:

    # this allows overwriting settings during runtime (aka tests)
    uri = uri or settings.DATABASE_URI
    table = table or settings.DATABASE_TABLE
    return ClickhouseDriver(uri, table)
