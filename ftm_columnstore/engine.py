import logging
from collections.abc import Iterable
from functools import cache
from typing import Any

import pandas as pd
from clickhouse_driver import Client, dbapi
from nomenklatura.settings import STATEMENT_TABLE
from sqlalchemy import Select

from ftm_columnstore import settings

log = logging.getLogger(__name__)


def table_exists(e: Exception, table: str) -> bool:
    if f"Table default.{table} already exists" in str(e):
        return True
    if "projection with this name already exists" in str(e):
        return True
    return False


def get_compiled_query(q: Any) -> str:
    # FIXME: this is dangerous!
    if hasattr(q, "compile"):
        q = str(q.compile(compile_kwargs={"literal_binds": True}))
        q = q.replace("group_concat", "first_value")
    q = str(q)
    log.debug(q)
    return q


class Connection(dbapi.Connection):
    stream: bool = False

    def execute(self, q: Any, *args, **kwargs) -> dbapi.cursor.Cursor:
        cursor = self.cursor()
        q = get_compiled_query(q)
        cursor.execute(q, *args, **kwargs)
        return cursor

    def execution_options(self, *args, **kwargs) -> "Connection":
        self.stream = kwargs.get("stream_results", False)
        return self


class ClickhouseDialect:
    name = "postgres"


class ClickhouseEngine:
    def __init__(
        self,
        uri: str | None = settings.DATABASE_URI,
    ):
        self.dialect = ClickhouseDialect()  # FIXME
        self.name = "clickhouse"
        self.table = STATEMENT_TABLE
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
        self.ensure(recreate=False, exists_ok=True)

    def __str__(self):
        return self.uri

    def __repr__(self):
        return f"<{self.__class__.__name__} ({self})>"

    def connect(self, use_numpy: bool | None = False) -> dbapi.Connection | Client:
        if use_numpy:
            uri = self.uri + "?use_numpy=True"
            return Client.from_url(uri)
        return Connection(self.uri)

    def ensure(self, recreate: bool | None = False, exists_ok: bool | None = False):
        with self.connect() as conn:
            if recreate:
                for stmt in self.drop_statements:
                    conn.execute(stmt)
            for stmt in self.create_statements:
                try:
                    conn.execute(stmt)
                except Exception as e:
                    if exists_ok and any(table_exists(e, t) for t in self.tables):
                        pass
                    else:
                        raise e
            # self.execute("GRANT ALL ON *.* TO CURRENT_USER WITH GRANT OPTION")

    def insert(self, df: pd.DataFrame, table: str | None = None) -> int:
        # https://clickhouse-driver.readthedocs.io/en/latest/features.html#numpy-pandas-support
        if df.empty:
            return 0
        table = table or self.table
        with self.connect(use_numpy=True) as conn:
            return conn.insert_dataframe("INSERT INTO %s VALUES" % table, df)

    def query_dataframe(self, query: Select) -> pd.DataFrame:
        query = get_compiled_query(query)
        with self.get_connection() as conn:
            return conn.query_dataframe(query)

    def sync(self):  # somehow not guaranteed by clickhouse
        with self.connect() as conn:
            conn.execute(f"OPTIMIZE TABLE {self.table} FINAL DEDUPLICATE")

    def optimize(self, full: bool | None = False):
        with self.connect() as conn:
            for table in (self.view_stats, self.view_fpx_freq):
                log.info(f"Optimizing `{table}` ...")
                conn.execute(f"OPTIMIZE TABLE {table} FINAL")
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
            INDEX dix (dataset) TYPE set(0) GRANULARITY 1,
            INDEX tix (prop_type) TYPE set(0) GRANULARITY 1,
            INDEX pix (prop) TYPE set(0) GRANULARITY 1
        ) ENGINE = ReplacingMergeTree(last_seen)
        PRIMARY KEY (canonical_id, entity_id, id)
        ORDER BY (canonical_id, entity_id, id)
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
            `user`                    String,
            INDEX lix (left_id) TYPE set(0) GRANULARITY 4,
            INDEX rix (right_id) TYPE set(0) GRANULARITY 4,
            INDEX ldix (left_dataset) TYPE set(0) GRANULARITY 1,
            INDEX rdix (right_dataset) TYPE set(0) GRANULARITY 1
        ) ENGINE = ReplacingMergeTree(ts)
        ORDER BY (left_id,right_id)
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
def get_engine(uri: str | None = None) -> ClickhouseEngine:
    uri = uri or settings.DATABASE_URI
    return ClickhouseEngine(uri)
