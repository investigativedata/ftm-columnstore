from typing import Generator, Iterable, Optional, Tuple, TypedDict

from followthemoney.schema import Schema
from nomenklatura.entity import CE
from nomenklatura.loader import MemoryLoader
from nomenklatura.xref import xref as _run_xref

from .dataset import Dataset
from .nk import ClickhouseLoader


class Match(TypedDict):
    left_id: str
    left_caption: str
    left_schema: str
    left_countries: str
    left_datasets: str
    right_id: str
    right_caption: str
    right_schema: str
    right_countries: str
    right_datasets: str
    judgement: str
    score: float


class NKxKwargs(TypedDict):
    range: Optional[Schema] = None
    schema: Optional[Schema] = None
    limit: int = 100_000
    scored: bool = True
    adjacent: bool = False
    auto_threshold: Optional[float] = None
    user: Optional[str] = None


MATCH_COLUMNS = tuple(Match.__annotations__.keys())


def run_xref(
    loader: ClickhouseLoader, **kwargs: NKxKwargs
) -> Generator[MemoryLoader, None, None]:
    for nk_loader in loader:
        kwargs.pop("auto_threshold", None)  # FIXME
        schema = kwargs.pop("schema", None)
        nk_kwargs: NKxKwargs = {**kwargs, **{"range": schema}}
        _run_xref(nk_loader, nk_loader.resolver, **nk_kwargs)
        yield nk_loader


def xref_dataset(
    dataset: Dataset,
    schema: Optional[Schema] = None,
    algorithm: Optional[str] = None,
    **kwargs: NKxKwargs,
) -> Generator[MemoryLoader, None, None]:
    loader = ClickhouseLoader([dataset], schema=schema, algorithm=algorithm)
    yield from run_xref(loader, **kwargs)


def xref_datasets(
    datasets: Iterable[Dataset],
    left_dataset: Optional[Dataset] = None,
    schema: Optional[Schema] = None,
    algorithm: Optional[str] = None,
    **kwargs: NKxKwargs,
) -> Generator[MemoryLoader, None, None]:
    loader = ClickhouseLoader(
        datasets, left_dataset, schema=schema, algorithm=algorithm
    )
    yield from run_xref(loader, **kwargs)


def get_candidates(
    result: Generator[MemoryLoader, None, None],
    as_entities: Optional[bool] = False,
    auto_threshold: Optional[float] = None,
    min_datasets: Optional[int] = 1,
    left_dataset: Optional[str] = None,
) -> Generator[Match | CE, None, None]:
    auto_threshold = auto_threshold or 0

    def _order(left: CE, right: CE) -> Tuple[CE, CE]:
        if left_dataset in right.datasets:
            return right, left
        return left, right

    for loader in result:
        resolver = loader.resolver
        for edge in resolver.edges.values():
            if auto_threshold < (edge.score or 0):
                left, right = loader.get_entity(edge.target.id), loader.get_entity(
                    edge.source.id
                )
                if left and right:
                    if len(left.datasets | right.datasets) >= min_datasets:
                        if left and right:
                            if as_entities:
                                yield left
                                yield right
                            else:
                                left, right = _order(left, right)
                                row: Match = {
                                    "left_id": left.id,
                                    "left_caption": left.caption,
                                    "left_schema": left.schema.name,
                                    "left_countries": ";".join(left.countries),
                                    "left_datasets": ";".join(left.datasets),
                                    "right_id": right.id,
                                    "right_caption": right.caption,
                                    "right_schema": right.schema.name,
                                    "right_countries": ";".join(right.countries),
                                    "right_datasets": ";".join(right.datasets),
                                    "judgement": edge.judgement.value,
                                    "score": edge.score,
                                }
                                yield row
