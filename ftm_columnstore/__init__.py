import logging
import warnings

import numpy as np

from .dataset import Dataset, get_dataset  # noqa
from .driver import get_driver  # noqa

# don't show numpy depcreaction warnings:
logging.getLogger("clickhouse_driver.columns.service").setLevel(logging.ERROR)
logging.getLogger("numpy.core.fromnumeric").setLevel(logging.ERROR)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=np.VisibleDeprecationWarning)
