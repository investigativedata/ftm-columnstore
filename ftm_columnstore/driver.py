from functools import lru_cache
from typing import Any, Iterable, Iterator, Optional

import pandas as pd
from clickhouse_driver import Client, errors

from . import enums, settings


def table_exists(e: Exception, table: str) -> bool:
    return f"Table default.{table} already exists" in str(e)


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
        table = table or self.table
        df = df.applymap(lambda x: None if x == "" or pd.isna(x) else x)
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
        UNIQUE = ("dataset", "entity_id", "schema", "prop", "origin", "value")
        CREATE_TABLE = """
        CREATE TABLE {table}
        (
            `id`                      FixedString(40) NOT NULL,
            `dataset`                 String NOT NULL,
            `entity_id`               String NOT NULL,
            `schema`                  {schema} NOT NULL,
            `origin`                  String NOT NULL,
            `prop`                    {prop} NOT NULL,
            `prop_type`               {prop_type} NOT NULL,
            `value`                   String NOT NULL,
            `value_num`               Decimal128(10) NULL,
            `last_seen`               DateTime NOT NULL
        ) ENGINE = ReplacingMergeTree(last_seen)
        PRIMARY KEY ({primary_key})
        ORDER BY ({order_by})
        """
        CREATE_TABLE_FPX = """
        CREATE TABLE {table}
        (
            `id`                      FixedString(40) NOT NULL,
            `dataset`                 String NOT NULL,
            `entity_id`               String NOT NULL,
            `schema`                  {schema} NOT NULL,
            `prop`                    {prop} NOT NULL,
            `fingerprint`             String NOT NULL,
            `fingerprint_id`          FixedString(40) NOT NULL,
            INDEX fp_ix (fingerprint) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
        ) ENGINE = ReplacingMergeTree()
        PRIMARY KEY ({primary_key})
        ORDER BY ({order_by})
        """

        def get_enum(values: Iterable[str]) -> str:
            values = ", ".join(f"'{v}'" for v in sorted(values))
            return f"Enum({values})"

        # implicit enum types
        schema = get_enum(enums.SCHEMATA)
        prop = get_enum(enums.PROPERTIES)
        prop_fpx = get_enum(enums.PROPERTIES_FPX)
        prop_type = get_enum(enums.PROPERTY_TYPES)
        primary_key = ", ".join(UNIQUE[:4])
        order_by = ", ".join(UNIQUE)
        create_table = CREATE_TABLE.format(
            table=self.table,
            schema=schema,
            prop=prop,
            prop_type=prop_type,
            primary_key=primary_key,
            order_by=order_by,
        )
        create_table_fpx = CREATE_TABLE_FPX.format(
            table=self.table_fpx,
            schema=schema,
            prop=prop_fpx,
            primary_key="fingerprint_id,schema,dataset",
            order_by="fingerprint_id,schema,dataset",
        )
        projection_values = f"""
        ALTER TABLE {self.table} ADD PROJECTION {self.table}_values (
            SELECT *
            ORDER BY value,prop,dataset,schema
        )
        """
        projection_entities = f"""
        ALTER TABLE {self.table} ADD PROJECTION {self.table}_entities (
            SELECT dataset,entity_id,schema,prop,groupUniqArray(value) as values
            GROUP BY dataset,entity_id,schema,prop
        )
        """
        return (
            create_table,
            create_table_fpx,
            projection_values,
            projection_entities,
        )

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
