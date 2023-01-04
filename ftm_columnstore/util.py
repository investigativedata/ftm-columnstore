import itertools
import logging
import re
from typing import Any, Iterable, Optional, Union

import pandas as pd
from click import File
from followthemoney import model
from followthemoney.proxy import E, EntityProxy
from followthemoney.schema import Schema
from nomenklatura.entity import CE, CompositeEntity

log = logging.getLogger(__name__)


def read_csv(
    infile: Union[File, str],
    do_raise: Optional[bool] = True,
    fillna: Optional[Any] = "",
    delimiter: Optional[str] = ",",
    **kwargs,
) -> pd.DataFrame:
    read_kwargs = {**{"on_bad_lines": "warn"}, **kwargs}
    df = None
    if isinstance(infile, str):
        fname = infile
    else:
        fname = infile.name

    try:
        if fname.endswith(".gz"):
            df = pd.read_csv(fname, compression="gzip", **read_kwargs)
        else:
            df = pd.read_csv(infile, **read_kwargs)
    except Exception as e:
        handle_error(log, e, do_raise, fpath=infile.name)
    if df is not None:
        return df.fillna(fillna)


def handle_error(
    logger, e: Union[Exception, str], do_raise: bool, dataset: Optional[str] = None
):
    if isinstance(e, str):
        e = Exception(e)
    if do_raise:
        raise e
    msg = ""
    if dataset is not None:
        msg = f"[{dataset}] "
    logger.error(msg + f"{e.__class__.__name__}: `{e}`")


NUMERIC_US = re.compile(r"^-?\d+(?:,\d{3})*(?:\.\d+)?$")
NUMERIC_DE = re.compile(r"^-?\d+(?:\.\d{3})*(?:,\d+)?$")


def to_numeric(value: str):
    value = str(value).strip()
    try:
        value = float(value)
        if int(value) == value:
            return int(value)
        return value
    except ValueError:
        if re.match(NUMERIC_US, value):
            return to_numeric(value.replace(",", ""))
        if re.match(NUMERIC_DE, value):
            return to_numeric(value.replace(".", "").replace(",", "."))


def clean_int(value: int | float) -> str:
    # 1.0 -> 1
    return str(int(value))


def slicer(n: int, iterable: Iterable):
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def expand_schema(schema: str | Schema | None) -> set[Schema]:
    if schema is None:
        return
    schema = model.get(schema)
    schemata = schema.matchable_schemata
    schemata.add(schema)
    if not schema.matchable:
        schemata.update(schema.descendants)
    return schemata


def get_proxy(data: CE | E | dict[str, Any]) -> CE:
    if isinstance(data, CompositeEntity):
        return data
    if isinstance(data, EntityProxy):
        data = data.to_dict()
    return CompositeEntity.from_dict(model, data)
