from enum import StrEnum
from functools import lru_cache
from typing import Literal

from fingerprints import generate as fp
from followthemoney.types import registry
from ftmq.types import CE
from libindic.soundex import Soundex
from metaphone import doublemetaphone
from normality import WS

SX = Soundex()


class PhoneticAlgorithm(StrEnum):
    fingerprint = "fingerprint"
    metaphone1 = "metaphone1"
    metaphone2 = "metaphone2"
    soundex = "soundex"


TPhoneticAlgorithm = Literal[
    PhoneticAlgorithm.fingerprint,
    PhoneticAlgorithm.metaphone1,
    PhoneticAlgorithm.metaphone2,
    PhoneticAlgorithm.soundex,
]

DEFAULT_PHONETIC_ALGORITHM = PhoneticAlgorithm.fingerprint


@lru_cache(10_000_000)
def tokenize(value: str) -> set[str]:
    tokens = set()
    tokens.add(value)
    tokens.update([t for t in value.split(WS) if len(t) > 5])  # FIXME
    return tokens


@lru_cache(10_000_000)
def get_fingerprint(value: str) -> str:
    return fp(value) or ""


@lru_cache(10_000_000)
def get_metaphone(value: str) -> tuple[str]:
    return tuple(x or "" for x in doublemetaphone(value))


@lru_cache(10_000_000)
def get_soundex(value: str) -> str:
    return SX.soundex(value) or ""


@lru_cache(10_000_000)
def get_phonetics(
    value: str, algorithm: TPhoneticAlgorithm | None = DEFAULT_PHONETIC_ALGORITHM
) -> tuple[str]:
    value = get_fingerprint(value)  # fingerprint always
    if not value:
        return ("",)
    tokens = tokenize(value)
    if algorithm == PhoneticAlgorithm.fingerprint:
        return tuple(t for t in tokens)
    if algorithm == PhoneticAlgorithm.metaphone1:
        return tuple(get_metaphone(t)[0] for t in tokens)
    if algorithm == PhoneticAlgorithm.metaphone2:
        return tuple(get_metaphone(t)[1] for t in tokens)
    if algorithm == PhoneticAlgorithm.soundex:
        return tuple(get_soundex(t) for t in tokens)


def get_entity_fpx(
    entity: CE,
    algorithm: TPhoneticAlgorithm | None = DEFAULT_PHONETIC_ALGORITHM,
) -> set[str]:
    values = set()
    for value in entity.get_type_values(registry.get("name")):
        values.update(get_phonetics(value, algorithm))
    for value in entity.get_type_values(registry.get("label")):
        values.update(get_phonetics(value, algorithm))
    return values
