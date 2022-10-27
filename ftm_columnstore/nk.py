import logging
import uuid
from typing import Generator, Iterable, Iterator, Optional

import networkx as nx
from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.util import make_entity_id
from nomenklatura.dataset import Dataset as NKDataset
from nomenklatura.entity import CE, CompositeEntity
from nomenklatura.loader import MemoryLoader
from nomenklatura.resolver import Resolver

from .dataset import Dataset
from .driver import ClickhouseDriver, get_driver
from .exceptions import InvalidAlgorithm
from .query import Query
from .statements import FPX_ALGORITHMS
from .util import slicer

log = logging.getLogger(__name__)


class ClickhouseLoader:
    """Load entities from clickhouse by fingerprint chunks."""

    def __init__(
        self,
        datasets: Iterable[Dataset],
        resolver: Optional[Resolver[CE]] = None,
        driver: Optional[ClickhouseDriver] = None,
        schema: Optional[Schema] = None,
        algorithm: Optional[str] = "metaphone1",
    ) -> None:
        self.driver = driver or get_driver()
        self.datasets = datasets
        self.resolver = resolver or Resolver[CE]()
        self.schema = schema
        self.algorithm = algorithm

    @property
    def dataset(self):
        if len(self.datasets) == 1:
            name = self.datasets[0].name
            title = name.title()
        else:
            name = ",".join(sorted([d.name for d in self.datasets]))
            title = f"Merged: {name}"
        return NKDataset(name, title)

    def get_chunks(
        self, algorithm: Optional[str] = "metaphone1"
    ) -> Generator[Generator[CompositeEntity, None, None], None, None]:
        if algorithm not in FPX_ALGORITHMS:
            raise InvalidAlgorithm(algorithm)
        datasets = [s.name for s in self.datasets]
        q = (
            Query(self.driver.table_fpx)
            .select(
                f"{algorithm}_id, groupUniqArray(dataset) AS datasets, groupUniqArray(entity_id) AS ids"
            )
            .where(
                **{
                    "prop": "name",
                    f"{algorithm}_id__null": False,
                    "dataset__in": datasets,
                }
            )
            .group_by(f"{algorithm}_id")
            .having(
                **{"length(datasets)__gt": int(len(datasets) > 1), "length(ids)__gt": 1}
            )
        )

        def _get_entities(entity_ids):
            for chunk in slicer(1_000, entity_ids):
                for dataset in self.datasets:
                    entities = dataset.EQ.where(entity_id__in=chunk, schema=self.schema)
                    entities = (
                        {**e.to_dict(), **{"datasets": [dataset.name]}}
                        for e in entities
                    )
                    yield from (CompositeEntity.from_dict(model, e) for e in entities)

        for row in q:
            entity_ids = row[2]
            yield _get_entities(entity_ids)

    def get_loaders(self):
        for entities in self.get_chunks(self.algorithm):
            yield MemoryLoader(self.dataset, entities, self.resolver)

    def __iter__(self) -> Iterator[CE]:
        yield from self.get_loaders()

    def __repr__(self) -> str:
        return f"<ClickhouseLoader({self.driver}, {self.dataset})>"


def apply_nk(items: Iterable[Iterable[str]]) -> Iterator[tuple[str, str]]:
    G = nx.Graph()
    for canonical_id, entity_id in items:
        G.add_edge(canonical_id, entity_id)
    for components in nx.connected_components(G):
        canonical_id = make_entity_id(uuid.uuid4())
        for entity_id in components:
            if not entity_id.startswith("NK-"):
                yield canonical_id, entity_id
