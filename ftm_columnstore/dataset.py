import logging
from functools import cached_property, lru_cache
from typing import Iterator, List, Optional, Union

import pandas as pd
from followthemoney.proxy import E
from followthemoney.util import make_entity_id

from . import settings
from .driver import ClickhouseDriver, get_driver
from .enums import FLAG
from .exceptions import EntityNotFound
from .query import EntityQuery, Query
from .statements import (
    Statement,
    fingerprints_from_entity,
    statements_from_entity,
    stmt_key,
)
from .util import handle_error

log = logging.getLogger(__name__)


class BulkWriter:
    def __init__(
        self,
        dataset: "Dataset",
        origin: Optional[str] = None,
        table: Optional[str] = None,
        size: Optional[int] = settings.BULK_WRITE_SIZE,
        ignore_errors: Optional[bool] = False,
    ):
        self.dataset = dataset
        self.origin = origin or dataset.origin
        self.table = table or dataset.driver.table
        self.buffer = []
        self.size = size
        self.ignore_errors = ignore_errors

    def put(self, statement: Statement):
        self.buffer.append(statement)
        if len(self.buffer) % self.size == 0:
            self.flush()

    def flush(self):
        if not len(self.buffer):
            return
        try:
            df = pd.DataFrame(self.buffer)
            df["origin"] = self.origin
            res = self.dataset.driver.insert(df, table=self.table)
            log.info(f"[{self.dataset}] Write: {len(self.buffer)} statements.")
            self.buffer = []
            return res
        except Exception as e:
            handle_error(log, e, not self.ignore_errors)


class EntityBulkWriter(BulkWriter):
    def __init__(self, *args, **kwargs):
        self.with_fingerprints = kwargs.pop("with_fingerprints", True)
        super().__init__(*args, **kwargs)
        if self.with_fingerprints:
            # FIXME this is a bit hacky, having sub writers instances...
            kwargs["table"] = self.dataset.driver.table_fpx
            self.bulk_fpx = BulkWriter(*args, **kwargs)

    def put(self, entity: Union[dict, E]):
        if hasattr(entity, "to_dict"):
            entity = entity.to_dict()
        else:
            entity = dict(entity)
        for statement in statements_from_entity(
            entity, self.dataset.name, self.dataset.origin
        ):
            super().put(statement)
        if self.with_fingerprints:
            for statement in fingerprints_from_entity(entity, self.dataset.name):
                self.bulk_fpx.put(statement)

    def flush(self):
        if self.with_fingerprints:
            self.bulk_fpx.flush()
        return super().flush()


class Dataset:
    def __init__(
        self,
        name: str,
        origin: Optional[str] = None,
        driver: Optional[ClickhouseDriver] = None,
        ignore_errors: Optional[bool] = False,
    ):
        self.name = name
        self.origin = origin
        self.driver = driver or get_driver()
        self.ignore_errors = ignore_errors
        self.Q = Query(driver=driver).where(dataset=name, origin=origin)
        self.EQ = EntityQuery(driver=driver).where(dataset=name, origin=origin)
        self.FPQ = Query(driver=driver, from_=self.driver.table_fpx).where(dataset=name)

    def drop(self, sync: Optional[bool] = False):
        log.info("Dropping ftm-store: %s" % self.name)
        where = self.Q.where_part
        stmt = f"ALTER TABLE {self.driver.table} DELETE {where}"
        res = self._execute(stmt)
        if sync:
            self.driver.sync()
        return res

    def delete(
        self,
        canonical_id: Optional[str] = None,
        entity_id: Optional[str] = None,
        origin: Optional[str] = None,
        sync: Optional[bool] = False,
    ):
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
        entity: E,
        origin: Optional[str] = None,
        with_fingerprints: Optional[bool] = True,
    ):
        bulk = self.bulk(
            origin=origin or self.origin, with_fingerprints=with_fingerprints
        )
        bulk.put(entity)
        return bulk.flush()

    def bulk(
        self, origin: Optional[str] = None, with_fingerprints: Optional[bool] = True
    ) -> "EntityBulkWriter":
        return EntityBulkWriter(
            self,
            with_fingerprints=with_fingerprints,
            origin=origin or self.origin,
            ignore_errors=self.ignore_errors,
        )

    @cached_property
    def bulk_statements(self) -> "BulkWriter":
        return BulkWriter(self, self.origin, ignore_errors=self.ignore_errors)

    def iterate(
        self,
        canonical_id: Optional[str] = None,
        entity_id: Optional[str] = None,
        origin: Optional[str] = None,
        chunksize: Optional[int] = 1000,
        schema: Optional[List[str]] = None,
        limit: Optional[int] = None,
    ) -> Iterator[E]:
        q = self.EQ
        if canonical_id is not None:
            q = q.where(canonical_id=canonical_id)
        if entity_id is not None:
            q = q.where(entity_id=entity_id)
        if origin is not None:
            q = q.where(origin=origin)
        if schema is not None:
            q = q.where(schema__in=schema)
        if limit is not None:
            q = q[:limit]
        return q.iterate(chunksize=chunksize)

    def statements(self, origin: Optional[str] = None) -> Iterator[tuple]:
        origin = origin or self.origin
        q = self.Q.select("DISTINCT ON (id) *").where(dataset=self.name, origin=origin)
        return q.iterate()

    def get(self, id_: str, canonical: Optional[bool] = True) -> E:
        if canonical:
            canonical_id = self.get_canonical_id(id_)
            for entity in self.iterate(canonical_id=canonical_id):
                return entity
        else:
            for entity in self.iterate(entity_id=id_):
                return entity

    def get_canonical_id(self, id_: str, origin: Optional[str] = None) -> str:
        origin = origin or self.origin
        for res in (
            self.Q.select("canonical_id")
            .where(entity_id=id_, origin=origin, sflag__not=FLAG.canonized.value)
            .order_by("ts", ascending=False)[0]
        ):
            return res[0]
        # otherwise id_ is already canonized:
        if self.Q.where(canonical_id=id_, origin=origin).exists():
            return id_
        raise EntityNotFound(id_)

    def canonize(
        self, entity_id: str, canonical_id: str, sync: Optional[bool] = False
    ) -> int:
        """
        set canonical id for entity and all references, add flag for old statements
        to exclude from default query
        """
        entity_id = str(entity_id)
        entity = self.get(entity_id)
        # first write references to avoid race condition
        bulk = self.bulk_statements
        # update_bulk = self.bulk_statements_canonize
        for e in self.expand(entity):
            new_ref_id = make_entity_id(canonical_id, e.id)
            for stmt in statements_from_entity(e, self.name, self.origin):
                # write old stmt as canonized
                old_stmt = stmt.copy()
                old_stmt["sflag"] = FLAG.canonized.value
                bulk.put(old_stmt)
                # write new canonized stmt
                stmt["canonical_id"] = new_ref_id
                if stmt["prop_type"] == "entity" and stmt["value"] == entity_id:
                    stmt["value"] = canonical_id
                stmt["id"] = stmt_key(**stmt)
                bulk.put(stmt)

        # write new entity statements
        for stmt in statements_from_entity(
            entity, self.name, self.origin, canonical_id
        ):
            bulk.put(stmt)

        # mark write old entity statements as canonized
        for stmt in statements_from_entity(
            entity, self.name, self.origin, sflag=FLAG.canonized.value
        ):
            bulk.put(stmt)

        if sync:
            # make sure dedupe happens in sync
            bulk.flush()
            self.driver.sync()

    def expand(self, entity: E, levels: Optional[int] = 1) -> Iterator[E]:
        """
        find connected entities in both directions
        """

        def _expand(entity: E, levels: int) -> Iterator[E]:
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

    def resolve(self, entity: E, levels: Optional[int] = 1) -> Iterator[E]:
        """
        resolve entity prop values to actual entites
        """

        def _resolve(entity: E, levels: int) -> Iterator[E]:
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
        return self.iterate()

    def __len__(self):
        return len(self.EQ)

    def __str__(self):
        return self.name

    def __repr__(self):
        return "<Dataset(%r, %r)>" % (self.driver, self.name)

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


@lru_cache(128)
def get_dataset(
    name: str,
    driver: Optional[ClickhouseDriver] = None,
    origin: Optional[str] = None,
    ignore_errors: Optional[bool] = False,
):
    driver = driver or get_driver()
    return Dataset(name, origin, driver, ignore_errors)
