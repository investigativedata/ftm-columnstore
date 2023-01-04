import logging
from typing import Iterable, Iterator, Optional

import click
from followthemoney import model
from followthemoney_typepredict.sampler import FastTextSampler
from nomenklatura.entity import CE

from .query import EntityQuery

log = logging.getLogger(__name__)

SCHEMAS = (
    model.schemata["Person"].name,
    model.schemata["Organization"].name,
    model.schemata["Company"].name,
    model.schemata["PublicBody"].name,
)

DEFAULT_FIELDS_LIMIT = {k: 1_000_000 for k in SCHEMAS}


class Sampler(FastTextSampler):
    def close(self):
        for schema, items in self._samplers.items():
            log.info(f"{schema}: using {len(items)} names")
        super().close()


def transform_proxy(proxy: CE, fields):
    if proxy.schema.name in SCHEMAS:
        yield from ((proxy.schema.name, value) for value in proxy.names)


def get_sampler(output_dir: click.Path) -> Sampler:
    return Sampler(
        output_dir, proxy_transformer=transform_proxy, fields_limit=DEFAULT_FIELDS_LIMIT
    )


def get_sample_entities(
    limit: Optional[int] = 1_000_000, datasets: Optional[Iterable[str]] = None
) -> Iterator[CE]:
    q = EntityQuery()
    if datasets is not None and len(datasets):
        q = q.where(dataset__in=datasets)
    for schema in SCHEMAS:
        yield from q.where(schema=schema)[:limit].iterate()
