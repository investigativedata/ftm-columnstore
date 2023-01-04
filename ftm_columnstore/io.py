# shorthands for import

import json
from typing import Union

from followthemoney import model

from .dataset import DS, get_dataset
from .driver import ClickhouseDriver, get_driver
from .exceptions import DatasetException


def import_json(
    fpath: str,
    dataset: Union[str, DS],
    origin: str | None = None,
    driver: ClickhouseDriver | None = None,
    **kwargs,
) -> int:
    """
    import a json file containing ftm entities, 1 entity per line without comma
    separator (aka jsonlines)

    return: number of imported entities
    """
    driver = driver or get_driver()
    dataset = get_dataset(dataset, origin, driver, **kwargs)
    if dataset.writable:
        bulk = dataset.store.bulk()
        i = 0
        with open(fpath) as f:
            for line in f.readlines():
                entity = json.loads(line)
                entity = model.get_proxy(entity)  # validation
                bulk.put(entity)
                i += 1
        bulk.flush()
        return i
    raise DatasetException(f"Dataset `{dataset}` not writable.")
