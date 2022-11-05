# https://github.com/opensanctions/opensanctions/blob/main/opensanctions/core/statements.py
from datetime import datetime
from functools import lru_cache
from hashlib import sha1
from typing import Iterator, Optional, TypedDict

import fingerprints.generate
from followthemoney import model
from followthemoney.util import make_entity_id
from libindic.soundex import Soundex
from metaphone import doublemetaphone

SX = Soundex()


class Statement(TypedDict):
    """A single statement about a property relevant to an entity.
    For example, this could be useddocker to say: "In dataset A, entity X has the
    property `name` set to 'John Smith'. I first observed this at K, and last
    saw it at L."
    Null property values are not supported. This might need to change if we
    want to support making property-less entities.
    """

    id: str
    dataset: str
    origin: str
    canonical_id: str
    entity_id: str
    schema: str
    prop: str
    prop_type: str
    value: str
    ts: datetime
    sflag: Optional[str] = None


class Fingerprints(TypedDict):
    algorithm: str
    value: str


class FingerprintStatement(Fingerprints):
    """A statement describing fingerprints and phonetic algorithms for an
    entity, useful for matching"""

    dataset: str
    entity_id: str
    schema: str


@lru_cache(1_000_000)
def fingerprint(value: str) -> Fingerprints:
    fingerprint = fingerprints.generate(value)
    fingerprint_id = make_entity_id(fingerprint)
    soundex = SX.soundex(value)
    metaphone1, metaphone2 = doublemetaphone(value)
    metaphone2 = metaphone2 or ""
    fingerprint_id = make_entity_id(fingerprint)
    fingerprint: Fingerprints = {
        "algorithm": "fingerprint",
        "value": fingerprint,
    }
    fingerprint_id: Fingerprints = {
        "algorithm": "fingerprint_id",
        "value": fingerprint_id,
    }
    soundex: Fingerprints = {"algorithm": "soundex", "value": soundex}
    metaphone1: Fingerprints = {"algorithm": "metaphone1", "value": metaphone1}
    metaphone2: Fingerprints = {"algorithm": "metaphone2", "value": metaphone2}
    return [fingerprint, fingerprint_id, soundex, metaphone1, metaphone2]


def stmt_key(
    dataset: str,
    entity_id: str,
    prop: str,
    value: str,
    origin: Optional[str] = "",
    **kwargs,
) -> str:
    """Hash the key properties of a statement record to make a unique ID."""
    key = f"{dataset}.{origin}.{entity_id}.{prop}.{value}"
    return sha1(key.encode("utf-8")).hexdigest()


@lru_cache(100_000)
def _denamespace(value: str) -> str:
    # de-namespacing? #FIXME
    return value.rsplit(".", 1)[0]


def statements_from_entity(
    entity: dict,
    dataset: str,
    origin: Optional[str] = "",
    canonical_id: Optional[str] = None,
    sflag: Optional[str] = None,
) -> Iterator[Statement]:
    entity = model.get_proxy(entity)
    if entity.id is None or entity.schema is None:
        return []
    entity_id = _denamespace(str(entity.id))
    canonical_id = canonical_id or entity_id
    stub: Statement = {
        "id": stmt_key(dataset, origin, entity_id, "id", entity_id),
        "dataset": dataset,
        "canonical_id": canonical_id,
        "entity_id": entity_id,
        "schema": entity.schema.name,
        "prop": "id",
        "prop_type": "id",
        "value": entity_id,
        "ts": datetime.now(),
        "sflag": sflag,
    }
    yield stub
    for prop, value in entity.itervalues():
        if value:
            if prop.type.name == "entity":
                value = _denamespace(value)
            stmt: Statement = {
                "id": stmt_key(dataset, origin, entity_id, prop.name, value),
                "dataset": dataset,
                "canonical_id": canonical_id,
                "entity_id": entity_id,
                "schema": entity.schema.name,
                "prop": prop.name,
                "prop_type": prop.type.name,
                "value": value,
                "ts": datetime.now(),
                "sflag": sflag,
            }
            yield stmt


def _should_fingerprint(entity):
    if entity.id is None or entity.schema is None:
        return False
    if entity.schema.is_a("Mention"):
        return True
    return entity.schema.is_a("Thing")


def fingerprints_from_entity(
    entity: dict, dataset: str
) -> Iterator[FingerprintStatement]:
    entity = model.get_proxy(entity)
    if not _should_fingerprint(entity):
        return []
    entity_id = entity.id.rsplit(".", 1)[0]
    for prop, value in entity.itervalues():
        if value:
            if prop.type.name == "name":
                fingerprints: [Fingerprints] = fingerprint(value)
                for fp in fingerprints:
                    if fp["value"]:
                        stmt: FingerprintStatement = {
                            **{
                                "dataset": dataset,
                                "entity_id": entity_id,
                                "schema": entity.schema.name,
                                "prop": prop.name,
                            },
                            **fp,
                        }
                        yield stmt


COLUMNS = tuple(Statement.__annotations__.keys())
COLUMNS_FPX = tuple(FingerprintStatement.__annotations__.keys())
FPX_ALGORITHMS = (
    "fingerprint",
    "fingerprint_id",
    "soundex",
    "metaphone1",
    "metaphone2",
)
