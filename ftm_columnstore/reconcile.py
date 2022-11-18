from typing import Optional

from fingerprints import generate as fp
from followthemoney.proxy import E

from .driver import ClickhouseDriver, get_driver
from .exceptions import InvalidAlgorithm
from .query import Query

DEFAULT_ALGORITHM = "fingerprint"


def guess_schema(
    entity: E,
    algorithm: Optional[str] = DEFAULT_ALGORITHM,
    driver: Optional[ClickhouseDriver] = None,
) -> dict:
    if algorithm != DEFAULT_ALGORITHM:
        raise InvalidAlgorithm(algorithm)
    driver = driver or get_driver()
    fingerprints = [fp(n) for n in entity.names]
    q = Query(driver.view_fpx_schemas).where(
        algorithm=algorithm, value__in=fingerprints
    )
    df = driver.query_dataframe(q)
    df = df.groupby("schema").agg({"schema_count": "sum"})
    df["freq"] = df["schema_count"] / df["schema_count"].sum()
    for schema, row in df.sort_values("freq", ascending=False).iterrows():
        return schema, row["freq"]
