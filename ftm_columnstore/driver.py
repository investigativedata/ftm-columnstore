import logging
from collections.abc import Iterable, Iterator
from functools import cache
from typing import Any

import pandas as pd
from clickhouse_driver import Client, errors
from nomenklatura.db import DB_STORE_TABLE
from sqlalchemy import CompoundSelect, Select

from ftm_columnstore import settings

log = logging.getLogger(__name__)


def table_exists(e: Exception, table: str) -> bool:
    if f"Table default.{table} already exists" in str(e):
        return True
    if "projection with this name already exists" in str(e):
        return True
    return False


class ClickhouseDriver:
    def __init__(
        self,
        uri: str | None = settings.DATABASE_URI,
    ):
        self.table = DB_STORE_TABLE
        self.table_fpx = f"{self.table}_fpx"
        self.table_xref = f"{self.table}_xref"
        self.view_stats = f"{self.table}_stats"
        self.view_fpx_freq = f"{self.table}_fpx_freq"
        self.tables = (
            self.table,
            self.table_fpx,
            self.table_xref,
            self.view_stats,
            self.view_fpx_freq,
        )
        self.uri = uri
        self.ensure_table()

    def __str__(self):
        return self.uri

    def __repr__(self):
        return f"<{self.__class__.__name__} ({self})>"

    def init(self, recreate: bool | None = False, exists_ok: bool | None = False):
        if recreate:
            self.dangerous_drop()
        for stmt in self.create_statements:
            try:
                self.execute(stmt)
            except Exception as e:
                if exists_ok and any(table_exists(e, t) for t in self.tables):
                    pass
                else:
                    raise e
        # self.execute("GRANT ALL ON *.* TO CURRENT_USER WITH GRANT OPTION")

    def dangerous_drop(self):
        for stmt in self.drop_statements:
            self.execute(stmt)

    def ensure_table(self):
        try:
            self.init(recreate=False)
        except errors.ServerException as e:
            if any(table_exists(e, t) for t in self.tables):
                pass
            else:
                raise e

    def get_compiled_query(self, s: Select | str) -> str:
        if isinstance(s, (Select, CompoundSelect)):
            s = str(s.compile(compile_kwargs={"literal_binds": True}))
            s = s.replace("group_concat", "first_value")
        log.debug(s)
        return s

    def insert(self, df: pd.DataFrame, table: str | None = None) -> int:
        # https://clickhouse-driver.readthedocs.io/en/latest/features.html#numpy-pandas-support
        if df.empty:
            return 0
        table = table or self.table
        with self.get_connection() as conn:
            res = conn.insert_dataframe("INSERT INTO %s VALUES" % table, df)
        return res

    def execute_iter(self, query: Select | str, *args, **kwargs) -> Iterator[Any]:
        query = self.get_compiled_query(query)
        conn = self.get_connection()
        kwargs = {**{"settings": {"max_block_size": 100000}}, **kwargs}
        return conn.execute_iter(query, *args, **kwargs)
        # with self.get_connection() as conn:
        #     res = conn.execute_iter(query, settings={"max_block_size": 100000})
        # return res

    def execute(self, query: Select | CompoundSelect | str, *args, **kwargs):
        query = self.get_compiled_query(query)
        with self.get_connection() as conn:
            return conn.execute(query, *args, **kwargs)

    def query_dataframe(self, query: Select) -> pd.DataFrame:
        query = self.get_compiled_query(query)
        with self.get_connection() as conn:
            return conn.query_dataframe(query)

    def get_connection(
        self,
        uri: str | None = settings.DATABASE_URI,
    ):
        uri = uri + "?use_numpy=True"
        return Client.from_url(uri)

    def sync(self):  # somehow not guaranteed by clickhouse
        self.execute(f"OPTIMIZE TABLE {self.table} FINAL DEDUPLICATE")

    def optimize(self, full: bool | None = False):
        for table in (self.view_stats, self.view_fpx_freq):
            log.info(f"Optimizing `{table}` ...")
            self.execute(f"OPTIMIZE TABLE {table} FINAL")
        if full:
            log.info(f"Optimizing `{self.table}` ...")
            self.sync()

    @property
    def create_statements(self) -> Iterable[str]:
        create_table = f"""
        CREATE TABLE {self.table}
        (
            `id`                      FixedString(40),
            `entity_id`               String,
            `canonical_id`            String,
            `prop`                    LowCardinality(String),
            `prop_type`               LowCardinality(String),
            `schema`                  LowCardinality(String),
            `value`                   String,
            `original_value`          Nullable(String),
            `dataset`                 LowCardinality(String),
            `lang`                    LowCardinality(String),
            `target`                  Boolean,
            `external`                Boolean,
            `first_seen`              Nullable(DateTime64),
            `last_seen`               DateTime64,
            INDEX cix (canonical_id) TYPE set(0) GRANULARITY 4,
            INDEX eix (entity_id) TYPE set(0) GRANULARITY 4,
            INDEX six (schema) TYPE set(0) GRANULARITY 1,
            INDEX tix (prop_type) TYPE set(0) GRANULARITY 1,
            INDEX pix (prop) TYPE set(0) GRANULARITY 1
        ) ENGINE = ReplacingMergeTree(last_seen)
        ORDER BY (canonical_id, id)
        """

        create_table_fpx = f"""
        CREATE TABLE {self.table_fpx}
        (
            `algorithm`     Enum('fingerprint', 'metaphone1', 'metaphone2', 'soundex'),
            `value`         String,
            `dataset`       LowCardinality(String),
            `entity_id`     String,
            `schema`        LowCardinality(String),
            `prop`          LowCardinality(String),
            `prop_type`     LowCardinality(String),
            INDEX eix (entity_id) TYPE set(0) GRANULARITY 4,
            INDEX six (schema) TYPE set(0) GRANULARITY 1,
            INDEX tix (prop_type) TYPE set(0) GRANULARITY 1,
            INDEX pix (prop) TYPE set(0) GRANULARITY 1
        ) ENGINE = ReplacingMergeTree()
        PRIMARY KEY (algorithm,value,prop,schema,dataset)
        ORDER BY (algorithm,value,prop,schema,dataset,entity_id)
        """

        create_table_xref = f"""
        CREATE TABLE {self.table_xref}
        (
            `left_dataset`            String,
            `left_id`                 String,
            `left_schema`             LowCardinality(String),
            `left_country`            LowCardinality(String),
            `left_caption`            String,
            `right_dataset`           String,
            `right_id`                String,
            `right_schema`            LowCardinality(String),
            `right_country`           LowCardinality(String),
            `right_caption`           String,
            `judgement`               LowCardinality(String),
            `score`                   Decimal32(8),
            `ts`                      DateTime64,
        ) ENGINE = ReplacingMergeTree(ts)
        PRIMARY KEY (left_dataset,left_schema)
        ORDER BY (left_dataset,left_schema,left_id,right_dataset,right_schema,right_id)
        """

        create_view_stats = f"""
        CREATE MATERIALIZED VIEW {self.view_stats} (
            dataset         LowCardinality(String),
            schema          LowCardinality(String),
            entities        AggregateFunction(count, UInt64),
            statements      AggregateFunction(count, UInt64)
        )
        ENGINE = AggregatingMergeTree()
        ORDER BY (dataset, schema)
        AS SELECT
            dataset,
            schema,
            countState(distinct canonical_id) AS entities,
            countState(*) AS statements
        FROM {self.table}
        GROUP BY dataset, schema
        """

        create_view_fpx_freq = f"""
        CREATE MATERIALIZED VIEW {self.view_fpx_freq} (
            value           String,
            freq            AggregateFunction(count, UInt32),
            len             UInt16
        )
        ENGINE = AggregatingMergeTree()
        ORDER BY (value)
        AS SELECT
            value,
            countState(value) AS freq,
            length(value) AS len
        FROM {self.table_fpx}
        WHERE algorithm = 'fingerprint'
        GROUP BY value
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
            f"""ALTER TABLE {self.table_fpx} ADD PROJECTION {self.table_fpx}_value (
                SELECT * ORDER BY value,schema,dataset)""",
            f"""ALTER TABLE {self.table_xref} ADD PROJECTION {self.table_xref}_reverse (
                SELECT * ORDER BY
            right_dataset,right_schema,right_id,left_dataset,left_schema,left_id)""",
        )
        return (
            create_table,
            create_table_fpx,
            create_table_xref,
            create_view_stats,
            create_view_fpx_freq,
            *projections,
        )

    @property
    def drop_statements(self) -> str:
        return (
            f"DROP TABLE IF EXISTS {self.table}",
            f"DROP TABLE IF EXISTS {self.table_fpx}",
            f"DROP TABLE IF EXISTS {self.table_xref}",
            f"DROP VIEW IF EXISTS {self.view_stats}",
            f"DROP VIEW IF EXISTS {self.view_fpx_freq}",
        )


@cache
def get_driver(uri: str | None = None) -> ClickhouseDriver:
    uri = uri or settings.DATABASE_URI
    return ClickhouseDriver(uri)
