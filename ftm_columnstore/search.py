from typing import Any, Generator

from fingerprints import generate as fp
from followthemoney.proxy import E
from followthemoney.types import registry
from nomenklatura.entity import CE

from .dataset import DataCatalog
from .phonetic import PhoneticAlgorithm, get_entity_fpx, tokenize
from .query import EntityQuery, Query
from .settings import SEARCH_LIMIT
from .util import get_proxy

ScoredFingerprint = tuple[str, float]
ScoredEntity = tuple[CE, float]


def _get_fpx_score_query(ds: DataCatalog, q: str) -> Query:
    return (
        Query(driver=ds.store.driver, from_=ds.store.driver.view_fpx_freq)
        .select(f"value, countMerge(freq) / len * {len(q)} / len AS score")
        .where(value__ilike=f"%{q}%", value__in=ds.store.FPQ.select("value"))
        .group_by("value", "len")
        .order_by("score", ascending=False)
    )


def get_fingerprint_scores(
    ds: DataCatalog, q: str
) -> Generator[ScoredFingerprint, None, None]:
    for value, score in _get_fpx_score_query(ds, q):
        yield value, float(score)


def get_result_entities(
    ds: DataCatalog,
    q: str,
    entity_query: EntityQuery,
    fingerprint_query: Query,
) -> Generator[ScoredEntity, None, None]:
    seen = set()
    for value, score in get_fingerprint_scores(ds, q):
        fpq = fingerprint_query.where(value=value)
        for entity in entity_query.where(canonical_id__in=str(fpq)):
            if entity.id not in seen:
                yield entity, score
                seen.add(entity.id)


def search_entities(
    q: str,
    ds: DataCatalog,
    query: EntityQuery | None = None,
    limit: int | None = SEARCH_LIMIT,
    fuzzy: bool | None = False,
) -> list[CE]:
    q = fp(q)
    if q is None:
        return []
    if query is None:
        query = ds.store.EQ
    fp_query = ds.store.FPQ.select("entity_id").where(algorithm="fingerprint")
    results: list[ScoredEntity] = []
    full = False
    seen = set()
    for entity, score in get_result_entities(ds, q, query, fp_query):
        if entity.id not in seen:
            results.append((entity, score))
            seen.add(entity.id)
            if len(results) == limit:
                full = True
                break

    # fuzzy: search for individual tokens if limit not reached
    if not full and fuzzy:
        tokens = sorted(tokenize(q), key=len, reverse=True)
        for token in tokens:
            for entity, score in get_result_entities(ds, token, query, fp_query):
                if entity.id not in seen:
                    results.append((entity, score / len(token)))
                    seen.add(entity.id)
                    if len(results) == limit:
                        full = True
                        break
            if full:
                break

    return results


def get_entity_match_query(
    ds: DataCatalog,
    entity: str | dict[str, Any] | CE | E,
) -> EntityQuery:
    if isinstance(entity, str):
        entity = ds.store.get(entity)
    else:
        entity = get_proxy(entity)

    fpx_values = set()
    for algorithm in PhoneticAlgorithm:
        fpx_values.update(get_entity_fpx(entity, algorithm.value))

    entity_ids = ds.store.FPQ.select("entity_id").where(value__in=fpx_values)
    filters = {}
    for prop, value in entity.itervalues():
        if (
            prop.type == registry.name
            or not prop.matchable  # noqa
            or prop.type in (registry.name, registry.address)  # noqa
        ):
            continue
        else:
            filters[prop.name] = value
    return ds.store.EQ.where(entity_id__in=entity_ids, **filters)
