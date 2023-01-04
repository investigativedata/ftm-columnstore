from clickhouse_driver.errors import Error as ClickhouseError  # noqa


class ImproperlyConfigured(Exception):
    pass


class InvalidQuery(Exception):
    pass


class EntityNotFound(Exception):
    pass


class InvalidAlgorithm(Exception):
    pass


class DatasetException(Exception):
    pass
