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

from .dataset import Dataset
from .exceptions import InvalidAlgorithm
from .query import Query
from .statements import FPX_ALGORITHMS
from .util import slicer

log = logging.getLogger(__name__)

DEFAULT_ALGORITHM = "metaphone1"


class ClickhouseLoader:
    """Load entities from clickhouse by fingerprint chunks."""

    def __init__(
        self,
        datasets: Iterable[Dataset],
        left_dataset: Optional[Dataset] = None,
        schema: Optional[Schema] = None,
        algorithm: Optional[str] = None,
    ) -> None:
        self.datasets = datasets
        self.left_dataset = left_dataset
        self.schema = schema
        self.algorithm = algorithm or DEFAULT_ALGORITHM
        self.driver = datasets[0].driver

    @property
    def dataset(self):
        if len(self.datasets) == 1:
            name = self.datasets[0].name
            title = name.title()
        else:
            name = ",".join(sorted([d.name for d in self.datasets]))
            title = f"Merged: {name}"
        return NKDataset(name, title)

    def query_distinct_fingerprints(
        self, dataset: Dataset, algorithm: Optional[str] = None
    ) -> Query:
        algorithm = algorithm or self.algorithm
        return (
            Query(self.driver.table_fpx)
            .select(f"DISTINCT {algorithm}")
            .where(dataset=dataset.name)
        )

    def get_query(
        self, algorithm: Optional[str] = None, left_dataset: Optional[Dataset] = None
    ) -> Query:
        algorithm = algorithm or self.algorithm
        left_dataset = left_dataset or self.left_dataset

        algorithm_lookup = {
            f"{algorithm}__null": False,
            f"{algorithm}__not": "",
        }

        datasets = [s.name for s in self.datasets]
        if left_dataset is not None and len(datasets) > 1:
            algorithm_lookup[f"{algorithm}__in"] = self.query_distinct_fingerprints(
                left_dataset, algorithm
            )
        return (
            Query(self.driver.table_fpx)
            .select(
                f"{algorithm}, groupUniqArray(dataset) AS datasets, groupUniqArray(entity_id) AS ids"
            )
            .where(prop="name", dataset__in=datasets, **algorithm_lookup)
            .group_by(algorithm)
            .having(
                **{"length(datasets)__gt": int(len(datasets) > 1), "length(ids)__gt": 1}
            )
        )

    def get_chunks(
        self, algorithm: Optional[str] = None, left_dataset: Optional[Dataset] = None
    ) -> Generator[Generator[CompositeEntity, None, None], None, None]:
        algorithm = algorithm or self.algorithm
        left_dataset = left_dataset or self.left_dataset

        if algorithm not in FPX_ALGORITHMS:
            raise InvalidAlgorithm(algorithm)

        def _get_entities(entity_ids):
            for chunk in slicer(1_000, entity_ids):
                for dataset in self.datasets:
                    entities = dataset.EQ.where(entity_id__in=chunk, schema=self.schema)
                    entities = (
                        {**e.to_dict(), **{"datasets": [dataset.name]}}
                        for e in entities
                    )
                    yield from (CompositeEntity.from_dict(model, e) for e in entities)

        for row in self.get_query(algorithm, left_dataset):
            entity_ids = row[2]
            yield _get_entities(entity_ids)

    def get_loaders(
        self, algorithm: Optional[str] = None, left_dataset: Optional[Dataset] = None
    ):
        algorithm = algorithm or self.algorithm
        left_dataset = left_dataset or self.left_dataset
        for entities in self.get_chunks(algorithm, left_dataset):
            yield MemoryLoader(self.dataset, entities)

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
