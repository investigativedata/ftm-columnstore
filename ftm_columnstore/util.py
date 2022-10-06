import logging
import re
from typing import Any, Optional, Union

import pandas as pd
from click import File

log = logging.getLogger("ftm_columnstore")


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
