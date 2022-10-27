from typing import Generator, Iterable, Optional, Tuple, TypedDict

from followthemoney.schema import Schema
from nomenklatura.entity import CE
from nomenklatura.resolver import Resolver
from nomenklatura.xref import xref as _run_xref

from .dataset import Dataset
from .driver import ClickhouseDriver, get_driver
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


MATCH_COLUMNS = tuple(Match.__annotations__.keys())


def run_xref(
    datasets: Iterable[Dataset],
    resolver: Resolver[CE],
    schema: Optional[Schema] = None,
    limit: int = 100_000,
    scored: bool = True,
    adjacent: bool = False,
    auto_threshold: Optional[float] = None,
    user: Optional[str] = None,
    driver: Optional[ClickhouseDriver] = None,
    algorithm: Optional[str] = None,
) -> Generator[Tuple[ClickhouseLoader, Resolver], None, None]:
    if driver is None:
        driver = get_driver()
    loaders = ClickhouseLoader(datasets, resolver, driver, schema, algorithm)
    for loader in loaders:
        resolver_ = Resolver()
        _run_xref(
            loader, resolver_, limit, scored, adjacent, schema, auto_threshold, user
        )
        yield loader, resolver_


def format_candidates(
    result: Generator[Tuple[ClickhouseLoader, Resolver], None, None],
    auto_threshold: Optional[float] = None,
    min_datasets: Optional[int] = 1,
    left_dataset: Optional[str] = None,
) -> Generator[Match, None, None]:
    auto_threshold = auto_threshold or 0

    def _order(left: CE, right: CE) -> Tuple[CE, CE]:
        if left_dataset in right.datasets:
            return right, left
        return left, right

    for loader, resolver in result:
        for edge in resolver.edges.values():
            if auto_threshold < (edge.score or 0):
                left, right = loader.get_entity(edge.target.id), loader.get_entity(
                    edge.source.id
                )
                if len(left.datasets | right.datasets) >= min_datasets:
                    if left and right:
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
