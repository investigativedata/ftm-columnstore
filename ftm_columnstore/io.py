# shorthands for import / export

import json
from typing import Optional

from followthemoney import model

from ftm_columnstore.dataset import Dataset
from ftm_columnstore.driver import ClickhouseDriver, get_driver


def import_json(
    fpath: str,
    dataset: str,
    with_fingerprints: Optional[bool] = False,
    origin: Optional[str] = None,
    driver: Optional[ClickhouseDriver] = None,
    **kwargs
) -> int:
    """
    import a json file containing ftm entities, 1 entity per line without comma
    separator (aka jsonlines)

    return: number of imported entities
    """
    driver = driver or get_driver()
    dataset = Dataset(dataset, origin, driver, **kwargs)
    bulk = dataset.bulk(with_fingerprints=with_fingerprints)
    i = 0
    with open(fpath) as f:
        for line in f.readlines():
            entity = json.loads(line)
            entity = model.get_proxy(entity)  # validation
            bulk.put(entity)
            i += 1
    bulk.flush()
    return i
