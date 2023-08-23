import logging
import warnings

import numpy as np
from nomenklatura import db

# shut up
logging.getLogger("clickhouse_driver.columns.service").setLevel(logging.ERROR)
logging.getLogger("numpy.core.fromnumeric").setLevel(logging.ERROR)
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=np.VisibleDeprecationWarning)

# FIXME sqlalchemy dummy patch
db.DB_URL = "sqlite:///:memory:"
