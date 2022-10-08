# https://github.com/opensanctions/opensanctions/blob/main/opensanctions/core/statements.py
from datetime import datetime
from functools import lru_cache
from hashlib import sha1
from typing import Iterator, TypedDict

import fingerprints.generate
from followthemoney import model
from followthemoney.util import make_entity_id


@lru_cache(maxsize=1024 * 1000)  # 1GB
def _get_fingerprint_id(fingerprint: str) -> str:
    return make_entity_id(fingerprint)


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
    canonical_id: str
    entity_id: str
    schema: str
    prop: str
    prop_type: str
    value: str
    last_seen: datetime


class FingerprintStatement(TypedDict):
    """A statement describing a fingerprint for an entity"""

    id: str
    dataset: str
    entity_id: str
    schema: str
    fingerprint: str
    fingerprint_id: str


def stmt_key(dataset: str, entity_id: str, prop: str, value: str) -> str:
    """Hash the key properties of a statement record to make a unique ID."""
    key = f"{dataset}.{entity_id}.{prop}.{value}"
    return sha1(key.encode("utf-8")).hexdigest()


def _denamespace(value: str) -> str:
    # de-namespacing? #FIXME
    return value.rsplit(".", 1)[0]


def _canonize(value: str) -> str:
    # always use sha1 id for canonical ids, convert entity id if it isn't sha1 yet
    if len(value) != 40:
        return make_entity_id(value)
    try:
        int(value, 16)
        return value
    except ValueError:
        return make_entity_id(value)


def statements_from_entity(entity: dict, dataset: str) -> Iterator[Statement]:
    entity = model.get_proxy(entity)
    if entity.id is None or entity.schema is None:
        return []
    entity_id = _denamespace(str(entity.id))
    canonical_id = _canonize(entity_id)
    stub: Statement = {
        "id": stmt_key(dataset, entity_id, "id", entity_id),
        "dataset": dataset,
        "canonical_id": canonical_id,
        "entity_id": entity_id,
        "schema": entity.schema.name,
        "prop": "id",
        "prop_type": "id",
        "value": entity_id,
        "last_seen": datetime.now().isoformat(),
    }
    yield stub
    for prop, value in entity.itervalues():
        if value:
            if prop.type.name == "entity":
                value = _canonize(_denamespace(value))
            stmt: Statement = {
                "id": stmt_key(dataset, entity_id, prop.name, value),
                "dataset": dataset,
                "canonical_id": canonical_id,
                "entity_id": entity_id,
                "schema": entity.schema.name,
                "prop": prop.name,
                "prop_type": prop.type.name,
                "value": value,
                "last_seen": datetime.now().isoformat(),
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
                fingerprint = fingerprints.generate(value)
                fingerprint_id = make_entity_id(fingerprint)
                stmt: FingerprintStatement = {
                    "id": stmt_key(dataset, entity_id, prop.name, value),
                    "dataset": dataset,
                    "entity_id": entity_id,
                    "schema": entity.schema.name,
                    "prop": prop.name,
                    "fingerprint": fingerprint,
                    "fingerprint_id": fingerprint_id,
                }
                yield stmt


COLUMNS = tuple(Statement.__annotations__.keys())
COLUMNS_FPX = tuple(FingerprintStatement.__annotations__.keys())
