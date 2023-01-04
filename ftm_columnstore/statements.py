from datetime import datetime
from functools import lru_cache
from typing import Any, Generator, Optional, TypedDict, TypeVar

from followthemoney.namespace import Namespace
from nomenklatura.statement import Statement
from nomenklatura.statement import StatementDict as NKStatementDict

from .phonetic import PhoneticAlgorithm, get_phonetics
from .util import get_proxy

SD = TypeVar("SD", bound="StatementDict")
FS = TypeVar("FS", bound="FingerprintStatement")


class StatementDict(NKStatementDict):
    origin: str
    sstatus: str


class Fingerprint(TypedDict):
    algorithm: str
    value: str


class FingerprintStatement(Fingerprint):
    """A statement describing fingerprints and phonetic algorithms for an
    entity, useful for pre-matching blocks"""

    dataset: str
    entity_id: str
    schema: str


@lru_cache(1_000_000)
def fingerprint(value: str) -> list[Fingerprint]:
    fingerprints: list[Fingerprint] = []
    for algorithm in PhoneticAlgorithm:
        for v in get_phonetics(value, algorithm.value):
            fingerprints.append({"algorithm": algorithm.value, "value": v})
    return fingerprints


def statements_from_entity(
    entity: dict,
    dataset: str,
    origin: Optional[str] = "",
    canonical_id: Optional[str] = None,
    status: Optional[str] = "",
) -> Generator[SD, None, None]:
    ns = Namespace()
    ts = datetime.now()
    proxy = get_proxy(entity)
    proxy.datasets.add(dataset)
    proxy = ns.apply(proxy)
    canonical_id = canonical_id or proxy.id
    for stmt in Statement.from_entity(proxy, dataset, first_seen=ts, last_seen=ts):
        stmt: SD = {
            **stmt.to_dict(),
            **{
                "origin": origin,
                "sstatus": status,
                "canonical_id": canonical_id,
            },
        }
        stmt["id"] = stmt_key(**stmt)
        yield stmt


def _should_fingerprint(entity):
    if entity.id is None or entity.schema is None:
        return False
    if entity.schema.is_a("Mention"):
        return True
    return entity.schema.is_a("LegalEntity")


def fingerprints_from_entity(
    entity: dict[str, Any], dataset: str
) -> Generator[FS, None, None]:
    ns = Namespace()
    entity = get_proxy(entity)
    entity = ns.apply(entity)
    if not _should_fingerprint(entity):
        return []
    for prop, value in entity.itervalues():
        if value:
            if prop.type.name == "name":
                fingerprints: list[Fingerprint] = fingerprint(value)
                for fp in fingerprints:
                    if fp["value"]:
                        stmt: FS = {
                            **{
                                "dataset": dataset,
                                "entity_id": entity.id,
                                "schema": entity.schema.name,
                                "prop": prop.name,
                                "prop_type": prop.type.name,
                            },
                            **fp,
                        }
                        yield stmt


def stmt_key(**data) -> str:
    return Statement.make_key(
        data["dataset"],
        data.get("canonical_id", data["entity_id"]),
        data["prop"],
        data["value"],
        data.get("external"),
    )


COLUMNS = tuple(Statement.__annotations__.keys())
COLUMNS_FPX = tuple(FingerprintStatement.__annotations__.keys())
