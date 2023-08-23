from collections.abc import Generator
from functools import cache
from typing import Any

import pandas as pd
from ftmq.model.dataset import C, Dataset
from ftmq.store import SqlQueryView, Store
from nomenklatura import store as nk
from nomenklatura.dataset import DS
from nomenklatura.db import get_statement_table
from nomenklatura.entity import CE
from nomenklatura.resolver import Resolver
from nomenklatura.statement import Statement
from nomenklatura.statement.serialize import pack_sql_statement
from sqlalchemy import select
from sqlalchemy.sql.selectable import Select

from ftm_columnstore.driver import get_driver
from ftm_columnstore.settings import BULK_WRITE_SIZE
from ftm_columnstore.statements import fingerprints_from_statements


class BaseClickhouseStore(nk.SqlStore):
    def __init__(
        self,
        dataset: DS,
        resolver: Resolver[CE],
        uri: str | None = None,
    ):
        super().__init__(dataset, resolver)
        self.table = get_statement_table()
        self.driver = get_driver(uri)
        self.columns = [c.name for c in self.table.columns]

    def writer(self) -> nk.Writer[DS, CE]:
        return ClickhouseWriter(self)

    def view(self, scope: DS, external: bool = False) -> nk.View[DS, CE]:
        return nk.sql.SqlView(self, scope, external=external)

    def _execute(
        self, q: Select, many: bool | None = True
    ) -> Generator[Any, None, None]:
        if many:
            yield from self.driver.execute_iter(q)
        else:
            yield from self.driver.execute(q)

    def _iterate_stmts(self, q: Select) -> Generator[Statement, None, None]:
        for row in self._execute(q):
            data = dict(zip(self.columns, row))
            yield Statement.from_dict(data)

    def _iterate(self, q: Select, *args, **kwargs) -> Generator[CE, None, None]:
        current_id = None
        current_stmts: list[Statement] = []
        for stmt in self._iterate_stmts(q):
            entity_id = stmt.entity_id
            if current_id is None:
                current_id = entity_id
            if current_id != entity_id:
                proxy = self.assemble(current_stmts)
                if proxy is not None:
                    yield proxy
                current_id = entity_id
                current_stmts = []
            current_stmts.append(stmt)
        if len(current_stmts):
            proxy = self.assemble(current_stmts)
            if proxy is not None:
                yield proxy


class ClickhouseStore(Store, BaseClickhouseStore):
    def query(self, scope: DS | None = None, external: bool = False) -> nk.View[DS, CE]:
        scope = scope or self.dataset
        return SqlQueryView(self, scope, external=external)


class ClickhouseWriter(nk.Writer[DS, CE]):
    def __init__(self, store: BaseClickhouseStore):
        self.store: ClickhouseStore = store
        self.batch: set[Statement] | None = None

    def flush(self) -> None:
        if self.batch:
            df = pd.DataFrame([pack_sql_statement(s) for s in self.batch])
            self.store.driver.insert(df, self.store.driver.table)
            df = pd.DataFrame(fingerprints_from_statements(self.batch))
            self.store.driver.insert(df, self.store.driver.table_fpx)
        self.batch = set()

    def add_statement(self, stmt: Statement) -> None:
        if self.batch is None:
            self.batch = set()
        if stmt.entity_id is None:
            return
        if len(self.batch) >= BULK_WRITE_SIZE:
            self.flush()
        stmt.canonical_id = self.store.resolver.get_canonical(stmt.entity_id)
        self.batch.add(stmt)

    def pop(self, entity_id: str) -> list[Statement]:
        self.flush()
        table = self.store.table
        q = select(table).where(table.c.entity_id == entity_id)
        statements: list[Statement] = list(self.store._iterate_stmts(q))
        # q_delete = delete(table).where(table.c.entity_id == entity_id)
        # stmt = str(q_delete.compile(compile_kwargs={"literal_binds": True}))
        # self.store.driver.execute(stmt)
        return statements


@cache
def get_store(
    uri: str | None = None,
    catalog: C | None = None,
    dataset: Dataset | str | None = None,
) -> Store:
    if isinstance(dataset, str):
        dataset = Dataset(name=dataset)
    return ClickhouseStore(catalog, dataset, uri=uri)
