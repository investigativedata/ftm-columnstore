import logging
from typing import Generator, Iterable

import networkx as nx
from followthemoney.schema import Schema
from nomenklatura.entity import CE
from nomenklatura.loader import MemoryLoader

from .dataset import Dataset
from .phonetic import DEFAULT_PHONETIC_ALGORITHM, TPhoneticAlgorithm
from .query import Query
from .util import expand_schema, slicer

log = logging.getLogger(__name__)


class ClickhouseLoader:
    """Load entities from clickhouse by fingerprint chunks."""

    def __init__(
        self,
        datasets: Iterable[Dataset],
        left_dataset: Dataset | None = None,
        schema: Schema | None = None,
        algorithm: TPhoneticAlgorithm | None = DEFAULT_PHONETIC_ALGORITHM,
    ) -> None:
        self.datasets = datasets
        self.left_dataset = left_dataset
        self.schemata = expand_schema(schema)
        self.algorithm = algorithm
        self.driver = datasets[0].store.driver

    @property
    def dataset(self):
        if len(self.datasets) == 1:
            return self.datasets[0]
        else:
            name = ",".join(sorted([d.name for d in self.datasets]))
            title = f"Merged: {name}"
            return Dataset(None, {"name": name, "title": title})

    def query_distinct_fingerprints(
        self, dataset: Dataset, algorithm: TPhoneticAlgorithm | None
    ) -> Query:
        algorithm = algorithm or self.algorithm
        return (
            Query(self.driver.table_fpx)
            .select("DISTINCT value")
            .where(dataset=dataset.name, algorithm=algorithm)
        )

    def get_query(
        self,
        algorithm: TPhoneticAlgorithm | None = None,
        left_dataset: Dataset | None = None,
    ) -> Query:
        algorithm = algorithm or self.algorithm
        left_dataset = left_dataset or self.left_dataset

        algorithm_lookup = {"algorithm": algorithm, "value__not": ""}

        datasets = [s.name for s in self.datasets]
        if left_dataset is not None and len(datasets) > 1:
            algorithm_lookup["value__in"] = self.query_distinct_fingerprints(
                left_dataset, algorithm
            )
        return (
            Query(self.driver.table_fpx)
            .select(
                "value, groupUniqArray(dataset) AS datasets, groupUniqArray(entity_id) AS ids"
            )
            .where(prop_type="name", dataset__in=datasets, **algorithm_lookup)
            .group_by("value")
            .having(
                **{"length(datasets)__gt": int(len(datasets) > 1), "length(ids)__gt": 1}
            )
        )

    def get_chunks(
        self,
        algorithm: TPhoneticAlgorithm | None = None,
        left_dataset: Dataset | None = None,
    ) -> Generator[Generator[CE, None, None], None, None]:
        algorithm = algorithm or self.algorithm
        left_dataset = left_dataset or self.left_dataset

        def _get_entities(entity_ids):
            for chunk in slicer(1_000, entity_ids):
                for dataset in self.datasets:
                    q = dataset.store.EQ.where(entity_id__in=chunk)
                    if self.schemata is not None:
                        q = q.where(schema__in=[s.name for s in self.schemata])
                    yield from q

        for row in self.get_query(algorithm, left_dataset):
            log.info(f"Blocking chunk: `{algorithm}` = `{row[0]}`")
            entity_ids = row[2]
            yield _get_entities(entity_ids)

    def get_loaders(
        self,
        algorithm: TPhoneticAlgorithm | None = None,
        left_dataset: Dataset | None = None,
    ):
        algorithm = algorithm or self.algorithm
        left_dataset = left_dataset or self.left_dataset
        for entities in self.get_chunks(algorithm, left_dataset):
            yield MemoryLoader(self.dataset, entities)

    def __iter__(self) -> Generator[CE, None, None]:
        yield from self.get_loaders()

    def __repr__(self) -> str:
        return f"<ClickhouseLoader({self.driver}, {self.dataset})>"


def apply_nk(items: Iterable[Iterable[str]]) -> Generator[tuple[str, str], None, None]:
    G = nx.Graph()
    for canonical_id, entity_id in items:
        G.add_edge(canonical_id, entity_id)
    for components in nx.connected_components(G):
        ids = sorted(components, key=len, reverse=True)
        canonical_id = ids.pop()
        for entity_id in ids:
            yield canonical_id, entity_id
