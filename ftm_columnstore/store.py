from collections.abc import Generator
from functools import cache

import pandas as pd
from ftmq.model.dataset import C, Dataset
from ftmq.store import SQLStore
from nomenklatura import store as nk
from nomenklatura.dataset import DS
from nomenklatura.db import get_metadata
from nomenklatura.entity import CE
from nomenklatura.resolver import Resolver
from nomenklatura.statement import Statement, make_statement_table
from sqlalchemy import MetaData, select
from sqlalchemy.sql.selectable import Select

from ftm_columnstore.engine import get_engine
from ftm_columnstore.settings import BULK_WRITE_SIZE
from ftm_columnstore.statements import fingerprints_from_statements


class BaseClickhouseStore(nk.SQLStore):
    def __init__(
        self,
        dataset: DS,
        linker: Resolver[CE],
        uri: str | None = None,
    ):
        super().__init__(dataset, linker)
        self.metadata = MetaData()
        self.table = make_statement_table(self.metadata)
        self.engine = get_engine(uri)
        self.columns = [c.name for c in self.table.columns]

    def writer(self) -> nk.Writer[DS, CE]:
        return ClickhouseWriter(self)

    def view(self, scope: DS, external: bool = False) -> nk.View[DS, CE]:
        return nk.sql.SQLView(self, scope, external=external)

    def _iterate_stmts(
        self, q: Select, *args, **kwargs
    ) -> Generator[Statement, None, None]:
        for row in self._execute(q):
            data = dict(zip(self.columns, row))
            yield Statement.from_dict(data)


class ClickhouseStore(SQLStore, BaseClickhouseStore):
    pass


class ClickhouseWriter(nk.sql.SQLWriter[DS, CE]):
    BATCH_STATEMENTS = BULK_WRITE_SIZE

    def _upsert_batch(self) -> None:
        if self.batch:
            df = pd.DataFrame([s.to_dict() for s in self.batch])
            self.store.engine.insert(df, self.store.engine.table)
            df = pd.DataFrame(fingerprints_from_statements(self.batch))
            self.store.engine.insert(df, self.store.engine.table_fpx)
        self.batch = set()

    def pop(self, entity_id: str) -> list[Statement]:
        self.flush()
        table = self.store.table
        q = select(table).where(table.c.entity_id == entity_id)
        statements: list[Statement] = list(self.store._iterate_stmts(q))
        # q_delete = delete(table).where(table.c.entity_id == entity_id)
        # stmt = str(q_delete.compile(compile_kwargs={"literal_binds": True}))
        # self.store.engine.execute(stmt)
        return statements


@cache
def get_store(
    uri: str | None = None,
    catalog: C | None = None,
    dataset: Dataset | str | None = None,
    resolver: Resolver | str | None = None,
) -> ClickhouseStore:
    get_metadata.cache_clear()
    if isinstance(dataset, str):
        dataset = Dataset(name=dataset)
    return ClickhouseStore(catalog, dataset, uri=uri, linker=resolver)
