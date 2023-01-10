import logging
from functools import lru_cache
from typing import Any, Iterable, Iterator, Optional

import pandas as pd
from clickhouse_driver import Client, errors

from . import settings

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
        uri: Optional[str] = settings.DATABASE_URI,
        table: Optional[str] = settings.DATABASE_TABLE,
    ):
        self.table = table
        self.table_fpx = f"{table}_fpx"
        self.table_xref = f"{table}_xref"
        self.view_stats = f"{table}_stats"
        self.view_fpx_freq = f"{table}_fpx_freq"
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
                    table_exists(e, self.table)
                    or table_exists(e, self.table_fpx)  # noqa
                    or table_exists(e, self.table_xref)  # noqa
                    or table_exists(e, self.view_stats)  # noqa
                    or table_exists(e, self.view_fpx_freq)  # noqa
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
            if (
                table_exists(e, self.table)
                or table_exists(e, self.table_fpx)  # noqa
                or table_exists(e, self.table_xref)  # noqa
                or table_exists(e, self.view_stats)  # noqa
                or table_exists(e, self.view_fpx_freq)  # noqa
            ):
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
        log.debug(query)
        conn = self.get_connection()
        kwargs = {**{"settings": {"max_block_size": 100000}}, **kwargs}
        return conn.execute_iter(query, *args, **kwargs)
        # with self.get_connection() as conn:
        #     res = conn.execute_iter(query, settings={"max_block_size": 100000})
        # return res

    def query_dataframe(self, query: Any) -> pd.DataFrame:
        query = str(query)
        log.debug(query)
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
        log.debug("\n".join(args))
        with self.get_connection() as conn:
            res = conn.execute(*args, **kwargs)
        return res

    def execute_iter(self, *args, **kwargs):
        return self.query(*args, **kwargs)

    def sync(self):  # somehow not guaranteed by clickhouse
        self.execute(f"OPTIMIZE TABLE {self.table} FINAL DEDUPLICATE")

    def optimize(self, full: Optional[bool] = False):
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
            `dataset`                 LowCardinality(String),
            `canonical_id`            String,
            `entity_id`               String,
            `schema`                  LowCardinality(String),
            `origin`                  LowCardinality(String),
            `prop`                    LowCardinality(String),
            `prop_type`               LowCardinality(String),
            `value`                   String,
            `lang`                    LowCardinality(String),
            `original_value`          Nullable(String),
            `target`                  Nullable(Boolean),
            `external`                Nullable(Boolean),
            `first_seen`              DateTime64,
            `last_seen`               DateTime64,
            `sstatus`                 LowCardinality(String),
            INDEX cix (canonical_id) TYPE set(0) GRANULARITY 4,
            INDEX eix (entity_id) TYPE set(0) GRANULARITY 4,
            INDEX six (schema) TYPE set(0) GRANULARITY 1,
            INDEX oix (origin) TYPE set(0) GRANULARITY 4,
            INDEX tix (prop_type) TYPE set(0) GRANULARITY 1,
            INDEX pix (prop) TYPE set(0) GRANULARITY 1
        ) ENGINE = ReplacingMergeTree(last_seen)
        PRIMARY KEY (dataset,schema,canonical_id)
        ORDER BY (dataset,schema,canonical_id,entity_id,origin,prop,value)
        """

        create_table_fpx = f"""
        CREATE TABLE {self.table_fpx}
        (
            `dataset`                 LowCardinality(String),
            `entity_id`               String,
            `schema`                  LowCardinality(String),
            `prop`                    LowCardinality(String),
            `prop_type`               LowCardinality(String),
            `algorithm`               Enum('fingerprint', 'metaphone1', 'metaphone2', 'soundex'),
            `value`                   String,
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
            f"""ALTER TABLE {self.table} ADD PROJECTION {self.table}_entities (
                SELECT groupUniqArray(dataset) AS datasets,canonical_id,schema,prop,groupUniqArray(value) AS values
                GROUP BY canonical_id,schema,prop)""",
            f"""ALTER TABLE {self.table_fpx} ADD PROJECTION {self.table_fpx}_value (
                SELECT * ORDER BY value,schema,dataset)""",
            f"""ALTER TABLE {self.table_xref} ADD PROJECTION {self.table_xref}_reverse (
                SELECT * ORDER BY right_dataset,right_schema,right_id,left_dataset,left_schema,left_id)""",
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


@lru_cache(128)
def get_driver(
    uri: Optional[str] = None,
    table: Optional[str] = None,
) -> ClickhouseDriver:

    # this allows overwriting settings during runtime (aka tests)
    uri = uri or settings.DATABASE_URI
    table = table or settings.DATABASE_TABLE
    return ClickhouseDriver(uri, table)
