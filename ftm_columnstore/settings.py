import os

VERSION = "0.2.0"


def get_env(name, default=None):
    value = os.environ.get(name)
    if value is not None:
        return str(value)
    if default is not None:
        return str(default)


DATABASE_URI = get_env("DATABASE_URI", "clickhouse://localhost/default")
LOG_LEVEL = get_env("LOG_LEVEL", "INFO")
BULK_WRITE_SIZE = int(get_env("BULK_WRITE_SIZE", 100_000))
