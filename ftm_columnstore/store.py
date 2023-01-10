from __future__ import annotations

import logging
from functools import cached_property
from typing import Any, Generator, Iterable

import pandas as pd
from followthemoney.util import make_entity_id
from nomenklatura.entity import CE

from . import settings
from .driver import ClickhouseDriver
from .enums import FLAG
from .exceptions import EntityNotFound
from .query import EntityQuery, Query
from .statements import (
    StatementDict,
    fingerprints_from_entity,
    statements_from_entity,
    stmt_key,
)
from .util import handle_error

log = logging.getLogger(__name__)

Entities = Generator[CE, None, None]
Statements = Generator[StatementDict, None, None]


class BulkWriter:
    def __init__(
        self,
        store: Any,  # FIXME
        origin: str | None = None,
        table: str | None = None,
        size: int | None = settings.BULK_WRITE_SIZE,
        ignore_errors: bool | None = False,
    ) -> None:
        self.store = store
        self.origin = origin or store.origin
        self.table = table or store.driver.table
        self.buffer = []
        self.size = size
        self.ignore_errors = ignore_errors

    def put(self, statement: StatementDict) -> None:
        self.buffer.append(statement)
        if len(self.buffer) % self.size == 0:
            self.flush()

    def flush(self) -> int | None:
        if not len(self.buffer):
            return
        try:
            df = pd.DataFrame(self.buffer)
            df["origin"] = self.origin
            res = self.store.driver.insert(df, table=self.table)
            log.info(f"[{self.store}] Write: {len(self.buffer)} statements.")
            self.buffer = []
            return res
        except Exception as e:
            handle_error(log, e, not self.ignore_errors)


class BulkUpdater:
    def __init__(
        self,
        store: Any,  # FIXME
        values: dict[str, Any],
        origin: str | None = None,
        table: str | None = None,
        size: int | None = settings.BULK_WRITE_SIZE,
        ignore_errors: bool | None = False,
    ) -> None:
        self.store = store
        self.origin = origin or store.origin
        self.table = table or store.driver.table
        self.buffer = set()
        self.size = size
        self.ignore_errors = ignore_errors
        self.values = values

    def put(self, statement: StatementDict) -> None:
        self.buffer.add(statement["id"])
        if len(self.buffer) % self.size == 0:
            self.flush()

    def flush(self) -> int | None:
        if not len(self.buffer):
            return
        try:
            q = Query(self.table).where(id__in=self.buffer)
            u = " ,".join((f"{c} = '{v}'" for c, v in self.values.items()))
            q = f"ALTER TABLE {self.table} UPDATE {u} {q.where_part}"
            res = self.store.driver.execute(q)
            log.info(f"[{self.store}] Update: {len(self.buffer)} statements.")
            self.buffer = set()
            return res
        except Exception as e:
            handle_error(log, e, not self.ignore_errors)


class EntityBulkWriter(BulkWriter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # FIXME this is a bit hacky, having sub writers instances...
        kwargs["table"] = self.store.driver.table_fpx
        self.bulk_fpx = BulkWriter(*args, **kwargs)

    def put(self, entity: dict[str, Any] | CE):
        if hasattr(entity, "to_dict"):
            entity = entity.to_dict()
        else:
            entity = dict(entity)
        for statement in statements_from_entity(
            entity, self.store.name, self.store.origin
        ):
            super().put(statement)
        # write fingerprints
        for statement in fingerprints_from_entity(entity, self.store.name):
            self.bulk_fpx.put(statement)

    def flush(self):
        self.bulk_fpx.flush()
        return super().flush()


class Store:
    def __init__(
        self,
        datasets: Iterable[str],
        driver: ClickhouseDriver,
        origin: str | None = None,
        ignore_errors: bool | None = False,
    ) -> None:
        self.datasets = sorted(datasets)
        self.origin = origin
        self.driver = driver
        self.ignore_errors = ignore_errors
        self.name = ",".join(datasets)

    @property
    def Q(self) -> Query:
        return Query(driver=self.driver).where(
            dataset__in=self.datasets, origin=self.origin
        )

    @property
    def EQ(self) -> EntityQuery:
        return EntityQuery(driver=self.driver).where(
            dataset__in=self.datasets, origin=self.origin
        )

    @property
    def FPQ(self) -> Query:
        return Query(driver=self.driver, from_=self.driver.table_fpx).where(
            dataset__in=self.datasets
        )

    def iterate(
        self,
        canonical_id: str | None = None,
        entity_id: str | None = None,
        origin: str | None = None,
        chunksize: int | None = 1000,
        schema: list[str] | None = None,
        limit: int | None = None,
    ) -> Entities:
        q = self.EQ
        if canonical_id is not None:
            q = q.where(canonical_id=canonical_id)
        if entity_id is not None:
            q = q.where(entity_id=entity_id)
        if origin is not None:
            q = q.where(origin=origin)
        if schema is not None and len(schema):
            q = q.where(schema__in=schema)
        if limit is not None:
            q = q[:limit]
        yield from q.iterate(chunksize=chunksize)

    def statements(self, origin: str | None = None) -> Statements:
        origin = origin or self.origin
        q = self.Q.select("DISTINCT ON (id) *").where(origin=origin)
        for stmt in q.iterate():
            yield stmt
            # yield Statement(**stmt)

    def get(self, id_: str, canonical: bool | None = True) -> CE:
        if canonical:
            canonical_id = self.get_canonical_id(id_)
            for entity in self.iterate(canonical_id=canonical_id):
                return entity
        else:
            for entity in self.iterate(entity_id=id_):
                return entity

    def get_canonical_id(self, id_: str, origin: str | None = None) -> str:
        origin = origin or self.origin
        for res in (
            self.Q.select("canonical_id")
            .where(entity_id=id_, origin=origin, sstatus__not=FLAG.canonized.value)
            .order_by("last_seen", ascending=False)[0]
        ):
            return res[0]
        # otherwise id_ is already canonized:
        if self.Q.where(canonical_id=id_, origin=origin).exists():
            return id_
        raise EntityNotFound(id_)

    def expand(self, entity: CE, levels: int | None = 1) -> Entities:
        """
        find connected entities in both directions
        """

        def _expand(entity: CE, levels: int) -> Entities:
            # outgoing
            yield from self.resolve(entity, levels)
            # incoming
            query = self.EQ.where(
                prop_type="entity", value=self.get_canonical_id(entity.id)
            )
            for entity in query:
                yield self.get(entity.id)  # ensure canonical entity
                if levels - 1 > 0:
                    yield from self.resolve(entity, levels - 1)
                    yield from _expand(entity, levels - 1)

        # uniq
        entities = set()
        for e in _expand(entity, levels):
            if e.id != entity.id:
                entities.add(e)
        yield from entities

    def resolve(self, entity: CE, levels: int | None = 1) -> Entities:
        """
        resolve entity prop values to actual entites
        """

        def _resolve(entity: CE, levels: int) -> Entities:
            for prop, value in entity.itervalues():
                if prop.type.name == "entity":
                    expanded_entity = self.get(value)
                    yield expanded_entity
                    if levels - 1 > 0:
                        yield from _resolve(expanded_entity, levels - 1)

        # uniq
        entities = set()
        for e in _resolve(entity, levels):
            if e.id != entity.id:
                entities.add(e)
        yield from entities

    def __iter__(self):
        yield from self.iterate()

    def __len__(self):
        return len(self.EQ)

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<%r(%r, %r)>" % (self.__class__, self.driver, self.name)

    def _execute(self, stmt: str):
        try:
            return self.driver.execute(stmt)
        except Exception as e:
            handle_error(
                log,
                e,
                not self.ignore_errors,
                dataset=self.name,
            )


class WriteStore(Store):
    def __init__(
        self,
        dataset: str,
        driver: ClickhouseDriver,
        origin: str | None = None,
        ignore_errors: bool | None = False,
    ) -> None:
        super().__init__([dataset], driver, origin, ignore_errors)
        self.dataset = dataset

    def drop(self, sync: bool | None = False):
        log.info("Dropping ftm-store: %s" % self.dataset)
        where = self.Q.where_part
        for stmt in (
            f"ALTER TABLE {self.driver.table} DELETE {where}",
            f"ALTER TABLE {self.driver.view_stats} DELETE {where}",
        ):
            self._execute(stmt)
        if sync:
            self.driver.sync()

    def delete(
        self,
        canonical_id: str | None = None,
        entity_id: str | None = None,
        origin: str | None = None,
        sync: bool | None = False,
    ):
        # FIXME need to update ftm stats view here
        q = self.Q
        filtered = False
        if canonical_id is not None:
            filtered = True
            q = q.where(canonical_id=canonical_id)
        if entity_id is not None:
            filtered = True
            q = q.where(entity_id=entity_id)
        if origin is not None:
            filtered = True
            q = q.where(origin=origin)
        if filtered:
            stmt = f"ALTER TABLE {self.driver.table} DELETE {q.where_part}"
            res = self._execute(stmt)
            if sync:
                self.driver.sync()
            return res
        res = self.drop()
        if sync:
            self.driver.sync()
        return res

    def put(
        self,
        entity: CE,
        origin: str | None = None,
    ):
        bulk = self.bulk(origin=origin or self.origin)
        bulk.put(entity)
        return bulk.flush()

    def bulk(self, origin: str | None = None) -> "EntityBulkWriter":
        return EntityBulkWriter(
            self,
            origin=origin or self.origin,
            ignore_errors=self.ignore_errors,
        )

    @cached_property
    def bulk_statements(self) -> "BulkWriter":
        return BulkWriter(self, self.origin, ignore_errors=self.ignore_errors)

    @cached_property
    def bulk_canonizer(self) -> "BulkUpdater":
        return BulkUpdater(
            self,
            {"sstatus": FLAG.canonized.value},
            self.origin,
            ignore_errors=self.ignore_errors,
        )

    def canonize(
        self, entity_id: str, canonical_id: str, sync: bool | None = False
    ) -> int:
        """
        set canonical id for entity and all references, add flag for old statements
        to exclude from default query
        """
        entity_id = str(entity_id)
        entity = self.get(entity_id)
        bulk = self.bulk_statements
        bulk_update = self.bulk_canonizer

        # first write references to avoid race condition
        for e in self.expand(entity):
            new_ref_id = make_entity_id(canonical_id, e.id)
            for stmt in statements_from_entity(e, self.dataset, self.origin):
                # update old stmt as canonized
                bulk_update.put(stmt)
                # write new canonized stmt
                stmt["canonical_id"] = new_ref_id
                if stmt["prop_type"] == "entity" and stmt["value"] == entity_id:
                    stmt["original_value"] = entity_id
                    stmt["value"] = canonical_id
                stmt["id"] = stmt_key(**stmt)
                bulk.put(stmt)

        # write new entity statements
        for stmt in statements_from_entity(
            entity, self.dataset, self.origin, canonical_id
        ):
            bulk.put(stmt)

        # mark old entity statements as canonized
        for stmt in statements_from_entity(entity, self.dataset, self.origin):
            bulk_update.put(stmt)

        if sync:
            # make sure dedupe happens in sync
            bulk.flush()
            bulk_update.flush()
            self.driver.sync()
