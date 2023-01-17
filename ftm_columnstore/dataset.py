from __future__ import annotations

import logging
from functools import lru_cache
from typing import Any, Generator, Iterable

from banal import is_listish
from nomenklatura.dataset import DataCatalog as NKDataCatalog
from nomenklatura.dataset import Dataset as NKDataset
from nomenklatura.entity import CE

from .driver import ClickhouseDriver, get_driver
from .query import Query
from .statements import Statement
from .store import Store, WriteStore

log = logging.getLogger(__name__)

Entities = Generator[CE, None, None]
Statements = Generator[Statement, None, None]
DS = "DataCatalog | Dataset"


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

    def __str__(self) -> str:
        return self.name

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

    def __str__(self) -> str:
        return ",".join(self.names)

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
