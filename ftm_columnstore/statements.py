from collections.abc import Generator, Iterable
from functools import cache, lru_cache
from typing import TypedDict, TypeVar

from followthemoney import model
from followthemoney.schema import Schema
from followthemoney.types import registry
from ftmq.types import CE
from nomenklatura.statement import Statement

from ftm_columnstore.phonetic import PhoneticAlgorithm, get_phonetics

FS = TypeVar("FS", bound="FingerprintStatement")

NAME_TYPE = str(registry.name)


class Fingerprint(TypedDict):
    algorithm: str
    value: str


class FingerprintStatement(Fingerprint):
    """A statement describing fingerprints and phonetic algorithms for an
    entity, useful for pre-matching blocks"""

    dataset: str
    entity_id: str
    schema: str
    prop: str
    prop_type: str

    @classmethod
    def from_row(cls, *values: str) -> "FingerprintStatement":
        return cls(zip(cls.__annotations__, values))


@cache
def get_schema(schema: str | Schema) -> Schema:
    return model.get(schema)


@lru_cache(1_000_000)
def fingerprint(value: str) -> list[Fingerprint]:
    fingerprints: list[Fingerprint] = []
    for algorithm in PhoneticAlgorithm:
        for v in get_phonetics(value, algorithm.value):
            fingerprints.append({"algorithm": algorithm.value, "value": v})
    return fingerprints


def should_fingerprint_stmt(stmt: Statement) -> bool:
    if not stmt.id or not stmt.schema or not stmt.value:
        return False
    schema = get_schema(stmt.schema)
    if schema.is_a("Mention") or schema.is_a("LegalEntity"):
        return stmt.prop_type == NAME_TYPE
    return False


def fingerprints_from_entity(entity: CE) -> Generator[FS, None, None]:
    yield from fingerprints_from_statements(entity.statements)


def fingerprints_from_statements(
    statements: Iterable[Statement],
) -> Generator[FS, None, None]:
    for stmt in statements:
        if should_fingerprint_stmt(stmt):
            for fp in fingerprint(stmt.value):
                if fp["value"]:
                    yield {
                        **{
                            "dataset": stmt.dataset,
                            "entity_id": stmt.entity_id,
                            "schema": stmt.schema,
                            "prop": stmt.prop,
                            "prop_type": stmt.prop_type,
                        },
                        **fp,
                    }


COLUMNS_FPX = tuple(FingerprintStatement.__annotations__.keys())
