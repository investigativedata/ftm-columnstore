# https://github.com/opensanctions/opensanctions/blob/main/opensanctions/core/statements.py
from datetime import datetime
from functools import lru_cache
from hashlib import sha1
from typing import Iterator, Optional, TypedDict

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
    entity_id: str
    schema: str
    prop: str
    prop_type: str
    value: str
    value_num: Optional[float] = None
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


def statements_from_entity(entity: dict, dataset: str) -> Iterator[Statement]:
    entity = model.get_proxy(entity)
    if entity.id is None or entity.schema is None:
        return []
    entity_id = entity.id.rsplit(".", 1)[0]
    for prop, value in entity.itervalues():
        if value:
            value_num = None
            if prop.type.name == "number":
                value_num = value
            elif prop.type.name == "entity":
                value = value.rsplit(".", 1)[0]
            stmt: Statement = {
                "id": stmt_key(dataset, entity_id, prop.name, value),
                "dataset": dataset,
                "entity_id": entity_id,
                "schema": entity.schema.name,
                "prop": prop.name,
                "prop_type": prop.type.name,
                "value": value,
                "value_num": value_num,
                "last_seen": datetime.now().isoformat(),
            }
            yield stmt


def fingerprints_from_entity(
    entity: dict, dataset: str
) -> Iterator[FingerprintStatement]:
    entity = model.get_proxy(entity)
    if (
        entity.id is None
        or entity.schema is None
        or not (entity.schema.is_a("Thing") or entity.schema.is_a("Mention"))
    ):
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
