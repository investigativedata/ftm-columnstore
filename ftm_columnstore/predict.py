from typing import Iterable, Optional

from followthemoney import model
from normality import normalize

from .query import EntityQuery

SCHEMAS = (
    model.schemata["Person"].name,
    model.schemata["Organization"].name,
    model.schemata["Company"].name,
    model.schemata["PublicBody"].name,
)


def get_sample_data(
    limit: Optional[int] = 1_000_000, datasets: Optional[Iterable[str]] = None
):
    q = EntityQuery()
    if datasets is not None and len(datasets):
        q = q.where(dataset__in=datasets)
    for schema in SCHEMAS:
        entities = q.where(schema=schema)[:limit].iterate()
        for entity in entities:
            for name in entity.names:
                yield schema, normalize(name, latinize=True, lowercase=True)
