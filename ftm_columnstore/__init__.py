import logging
import warnings

import nomenklatura.settings
import numpy as np

from ftm_columnstore.engine import get_engine
from ftm_columnstore.store import get_store

# shut up
logging.getLogger("clickhouse_driver.columns.service").setLevel(logging.ERROR)
logging.getLogger("numpy.core.fromnumeric").setLevel(logging.ERROR)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=np.VisibleDeprecationWarning)

# FIXME sqlalchemy monkey patch not working
nomenklatura.settings.DB_URL = "sqlite:///:memory:"

__version__ = "0.2.1"

__all__ = ["get_engine", "get_store"]
