import os


def get_env(name, default=None):
    value = os.environ.get(name)
    if value is not None:
        return str(value)
    if default is not None:
        return str(default)


DATABASE_URI = get_env("DATABASE_URI", "localhost")
DATABASE_TABLE = get_env("DATABASE_TABLE", "ftm")
LOG_LEVEL = get_env("LOG_LEVEL", "info")
LRU_QUERY_CACHE_SIZE = int(get_env("LRU_QUERY_CACHE_SIZE", 1024 * 1000))
BULK_WRITE_SIZE = int(get_env("BULK_WRITE_SIZE", 100_000))
