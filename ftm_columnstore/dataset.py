import logging
from functools import lru_cache
from typing import Iterator, Optional, Union

import pandas as pd
from followthemoney.proxy import E

from . import settings
from .driver import ClickhouseDriver, get_driver
from .query import EntityQuery, Query
from .statements import fingerprints_from_entity, statements_from_entity
from .util import handle_error

log = logging.getLogger(__name__)


class BulkWriter:  # FIXME make seperated fingerprints writer?
    def __init__(
        self,
        dataset: "Dataset",
        with_fingerprints: Optional[bool] = False,
        origin: Optional[str] = None,
        size: Optional[int] = settings.BULK_WRITE_SIZE,
        ignore_errors: Optional[bool] = False,
    ):
        self.dataset = dataset
        self.with_fingerprints = with_fingerprints
        self.origin = origin or dataset.origin
        self.buffer = []
        self.buffer_fpx = []
        self.size = size
        self.ignore_errors = ignore_errors
        self.entities = 0
        self.statements = 0
        self.statements_fpx = 0

    def put(self, entity: Union[dict, E]):
        if hasattr(entity, "to_dict"):
            entity = entity.to_dict()
        else:
            entity = dict(entity)
        self.entities += 1
        for statement in statements_from_entity(entity, self.dataset.name):
            self.buffer.append(statement)
            self.statements += 1
        if self.with_fingerprints:
            for statement in fingerprints_from_entity(entity, self.dataset.name):
                self.buffer_fpx.append(statement)
                self.statements_fpx += 1
        if self.entities % self.size == 0:
            self.flush()

    def flush(self):
        if not len(self.buffer):
            return
        try:
            df = pd.DataFrame(self.buffer)
            df["origin"] = self.origin
            res = self.dataset.driver.insert(df)
            log.info(
                f"[{self.dataset}] Write: {self.entities} entities with {self.statements} statements."
            )
            if self.with_fingerprints:
                df = pd.DataFrame(self.buffer_fpx)
                df["origin"] = self.origin
                self.dataset.driver.insert(df, table=self.dataset.driver.table_fpx)
                log.info(
                    f"[{self.dataset}] Write: {self.statements_fpx} fingerprint statements."
                )
            self.buffer = []
            self.buffer_fpx = []
            return res
        except Exception as e:
            handle_error(log, e, not self.ignore_errors)


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

    def drop(self):
        log.info("Dropping ftm-store: %s" % self.name)
        where = self.Q.where_part
        stmt = f"DELETE FROM {self.driver.table} {where}"
        try:
            return self.driver.execute(stmt)
        except Exception as e:
            handle_error(
                log,
                e,
                not self.ignore_errors,
                dataset=self.name,
            )

    def delete(
        self,
        canonical_id: Optional[str] = None,
        entity_id: Optional[str] = None,
        origin: Optional[str] = None,
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
            stmt = f"DELETE FROM {self.driver.table} {q.where_part}"
            return self.driver.execute(stmt)
        return self.drop()

    def put(self, entity, origin: Optional[str] = None):
        bulk = self.bulk(origin=origin or self.origin)
        bulk.put(entity)
        return bulk.flush()

    def bulk(
        self, origin: Optional[str] = None, with_fingerprints: Optional[bool] = False
    ) -> "BulkWriter":
        return BulkWriter(
            self,
            with_fingerprints=with_fingerprints,
            origin=origin or self.origin,
            ignore_errors=self.ignore_errors,
        )

    def iterate(
        self,
        canonical_id: Optional[str] = None,
        entity_id: Optional[str] = None,
        origin: Optional[str] = None,
        chunksize: Optional[int] = 1000,
    ) -> Iterator[E]:
        q = self.EQ
        if canonical_id is not None:
            q = q.where(canonical_id=canonical_id)
        if entity_id is not None:
            q = q.where(entity_id=entity_id)
        if origin is not None:
            q = q.where(origin=origin)
        return q.iterate(chunksize=chunksize)

    def statements(self, origin: Optional[str] = None) -> Iterator[tuple]:
        origin = origin or self.origin
        q = self.Q.select("DISTINCT ON (id) *").where(dataset=self.name, origin=origin)
        return q.iterate()

    def get(self, id_: str, canonical: Optional[bool] = True) -> E:
        if canonical:
            for entity in self.iterate(canonical_id=id_):
                return entity
        else:
            for entity in self.iterate(entity_id=id_):
                return entity

    def expand(self, entity: E, levels: Optional[int] = 1) -> Iterator[E]:
        """
        find connected entities in both directions
        """

        def _expand(entity: E, levels: int) -> Iterator[E]:
            # outgoing
            yield from self.resolve(entity, levels)
            # incoming
            query = EntityQuery(driver=self.driver).where(
                prop_type="entity", value=entity.id
            )
            for entity in query:
                yield entity
                if levels - 1 > 0:
                    yield from self.resolve(entity, levels - 1)
                    yield from _expand(entity, levels - 1)

        # uniq
        entities = set()
        for e in _expand(entity, levels):
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


@lru_cache(maxsize=128)
def get_dataset(
    name: str,
    driver: Optional[ClickhouseDriver] = None,
    origin: Optional[str] = None,
    ignore_errors: Optional[bool] = False,
):
    driver = driver or get_driver()
    return Dataset(name, origin, driver, ignore_errors)
