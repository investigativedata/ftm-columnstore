from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Generator, Iterable

import pandas as pd
from banal import is_listish
from nomenklatura.dataset import DataCatalog as NKDataCatalog
from nomenklatura.dataset import Dataset as NKDataset
from nomenklatura.entity import CE

from . import settings
from .driver import ClickhouseDriver, get_driver
from .query import Query
from .statements import Statement, fingerprints_from_entity, statements_from_entity
from .store import Store, WriteStore
from .util import handle_error

log = logging.getLogger(__name__)

Entities = Generator[CE, None, None]
Statements = Generator[Statement, None, None]
DS = "DataCatalog | Dataset"


class BulkWriter:
    def __init__(
        self,
        dataset: Dataset,
        origin: str | None = None,
        table: str | None = None,
        size: int | None = settings.BULK_WRITE_SIZE,
        ignore_errors: bool | None = False,
    ) -> None:
        self.dataset = dataset
        self.origin = origin or dataset.origin
        self.table = table or dataset.driver.table
        self.buffer = []
        self.size = size
        self.ignore_errors = ignore_errors

    def put(self, statement: Statement) -> None:
        self.buffer.append(statement)
        if len(self.buffer) % self.size == 0:
            self.flush()

    def flush(self) -> int | None:
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
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        # FIXME this is a bit hacky, having sub writers instances...
        kwargs["table"] = self.dataset.driver.table_fpx
        self.bulk_fpx = BulkWriter(*args, **kwargs)

    def put(self, entity: dict[str, Any] | CE):
        if hasattr(entity, "to_dict"):
            entity = entity.to_dict()
        else:
            entity = dict(entity)
        for statement in statements_from_entity(
            entity, self.dataset.name, self.dataset.origin
        ):
            super().put(statement)
        # write fingerprints
        for statement in fingerprints_from_entity(entity, self.dataset.name):
            self.bulk_fpx.put(statement)

    def flush(self):
        self.bulk_fpx.flush()
        return super().flush()


class Dataset(NKDataset):
    writable = True

    def __init__(
        self,
        catalog: DataCatalog[Dataset],
        data: dict[str, Any],
        origin: str | None = None,
        driver: ClickhouseDriver | None = None,
        ignore_errors: bool | None = False,
    ) -> None:
        data["title"] = data.get("title", data["name"].title())
        super().__init__(catalog, data)
        self.store = WriteStore(
            self.name, driver or get_driver(), origin, ignore_errors
        )

    @classmethod
    def from_name(
        cls,
        name: str,
        origin: str | None = None,
        driver: ClickhouseDriver | None = None,
        ignore_errors: bool | None = False,
    ) -> Dataset:
        return cls(None, {"name": name})


class DataCatalog(NKDataCatalog):
    writable = False

    def __init__(
        self,
        data: dict[str, Any],
        origin: str | None = None,
        driver: ClickhouseDriver | None = None,
        ignore_errors: bool | None = False,
    ) -> None:
        super().__init__(Dataset, data)
        self.store = Store(self.names, driver or get_driver(), origin, ignore_errors)

    @classmethod
    def from_names(
        cls,
        names: Iterable[str],
        origin: str | None = None,
        driver: ClickhouseDriver | None = None,
        ignore_errors: bool | None = False,
    ) -> DataCatalog:
        datasets = [{"name": name, "title": name.title()} for name in names]
        return cls({"datasets": datasets}, origin, driver, ignore_errors)


@lru_cache
def get_complete_catalog(driver: ClickhouseDriver | None = None) -> DataCatalog:
    driver = driver or get_driver()
    q = Query(from_=driver.table).select("DISTINCT dataset")
    names = [x[0] for x in driver.execute(str(q))]
    return DataCatalog.from_names(names)


@lru_cache
def get_dataset(
    name: Iterable[str] | str | DS,
    driver: ClickhouseDriver | None = None,
    origin: str | None = None,
    ignore_errors: bool | None = False,
) -> Dataset | DataCatalog:
    if isinstance(name, (Dataset, DataCatalog)):
        return name
    driver = driver or get_driver()
    args = origin, driver, ignore_errors
    if name == "*":
        return get_complete_catalog()
    if is_listish(name):
        return DataCatalog.from_names(name, *args)
    if "," in name:
        return DataCatalog.from_names(name.split(","), *args)
    return Dataset.from_name(name, *args)
