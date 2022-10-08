import itertools
from typing import Any, Iterable, Iterator, Optional, Union

import pandas as pd
from banal import as_bool, clean_dict, is_listish
from followthemoney import model
from followthemoney.proxy import E

from . import enums
from .driver import ClickhouseDriver, get_driver
from .exceptions import InvalidQuery

TYPES = {p.name: p.type.name for p in model.properties}


class Query:
    OPERATORS = {
        "like": "LIKE",
        "ilike": "ILIKE",
        "gt": ">",
        "gte": ">=",
        "lt": "<",
        "lte": "<=",
        "in": "IN",
        "null": "IS",
    }
    fields = None

    def __init__(
        self,
        from_: Optional[Union[str, "Query"]] = None,
        fields: Optional[Iterable[str]] = None,
        group_by_fields: Optional[Iterable[str]] = None,
        order_by_fields: Optional[Iterable[str]] = None,
        order_direction: Optional[str] = "ASC",
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        where_lookup: Optional[dict] = None,
        having_lookup: Optional[dict] = None,
        driver: Optional[ClickhouseDriver] = None,
    ):
        driver = driver or get_driver()
        if from_ is None:
            from_ = driver.table
        self.driver = driver
        self.fields = fields or self.fields
        self.from_ = f"({from_})" if isinstance(from_, Query) else from_
        self.group_by_fields = group_by_fields
        self.order_by_fields = order_by_fields
        self.order_direction = order_direction
        self.limit = limit
        self.offset = offset
        self.where_lookup = where_lookup
        self.having_lookup = having_lookup

    def __str__(self) -> str:
        return self.get_query()

    def __iter__(self) -> Iterator[Any]:
        return self.iterate()

    def __len__(self) -> int:
        return self.count()

    def iterate(self):
        return self.execute()

    def count(self):
        # FIXME this doesn't cover aggregated cases for `having`
        count_part = "*"
        if self.group_by_fields:
            count_part = f"DISTINCT {', '.join(self.group_by_fields)}"
        query = (
            f"SELECT count({count_part}) as count FROM {self.from_}{self.where_part}"
        )
        res = self.execute(query)
        for i in res:
            return i[0]

    def execute(self, query: Optional[Union["Query", str]] = None) -> Iterator[Any]:
        """return result iterator"""
        query = query or self.get_query()
        return self.driver.query(str(query))

    def first(self):
        # return the first object
        for res in self:
            return res

    def _chain(self, **kwargs):
        # merge current state
        new_kwargs = self.__dict__.copy()
        for key, new_value in kwargs.items():
            old_value = new_kwargs[key]
            if old_value is None:
                new_kwargs[key] = new_value
            # "remove" old value:
            elif new_value is None:
                new_kwargs[key] = None
            # overwrite order by
            elif key == "order_by_fields":
                new_kwargs[key] = new_value
            # combine iterables and dicts
            elif is_listish(old_value):
                new_kwargs[key] = sorted(set(old_value) | set(new_value))
            elif isinstance(old_value, dict):
                new_kwargs[key] = {**old_value, **new_value}
            else:  # replace
                new_kwargs[key] = new_value
        return self.__class__(**new_kwargs)

    def select(self, *fields) -> "Query":
        return self._chain(fields=fields)

    def where(self, **filters) -> "Query":
        return self._chain(where_lookup=filters)

    def having(self, **filters) -> "Query":
        return self._chain(having_lookup=filters)

    def group_by(self, *fields) -> "Query":
        return self._chain(group_by_fields=fields)

    def order_by(self, *fields, ascending=True) -> "Query":
        return self._chain(
            order_by_fields=fields, order_direction="ASC" if ascending else "DESC"
        )

    # for slicing
    def __getitem__(self, value) -> "Query":
        if isinstance(value, int):
            if value < 0:
                raise InvalidQuery("Invalid slicing: slice must not be negative.")
            return self._chain(limit=1, offset=value)
        if isinstance(value, slice):
            if value.step is not None:
                raise InvalidQuery("Invalid slicing: steps not allowed.")
            offset = value.start or 0
            if value.stop is not None:
                return self._chain(limit=value.stop - offset, offset=offset)
            return self._chain(offset=offset)
        raise NotImplementedError

    @property
    def fields_part(self) -> str:
        return ", ".join(self.fields or "*")

    def _get_lookup_part(
        self, lookup: dict, strict_fields: Optional[bool] = True
    ) -> str:
        """for where and having clause

        lookups for ftm properties (name="foo") will be rewritten as
        (prop="name" AND value="foo")
        """
        meta_fields = {
            "dataset",
            "schema",
            "origin",
            "canonical_id",
            "entity_id",
            "prop",
            "prop_type",
            "value",
            "fingerprint",
            "fingerprint_id",
        }
        meta_parts = set()
        parts = set()
        lookup = clean_dict(lookup)

        def _get_part(key: str, value: str, operator: Optional[str] = None) -> str:
            operator = operator or "="
            if key in enums.PROPERTIES:
                value_field = "value"
                if TYPES.get(key) == "number":
                    value_field = "value_num"
                return f"(prop = '{key}' AND {value_field} {operator} {value})"
            return f"{key} {operator} {value}"

        for field, value in lookup.items():
            field, *operator = field.split("__")

            if strict_fields:
                if field not in meta_fields | enums.PROPERTIES:
                    if field not in enums.PROPERTIES:
                        raise InvalidQuery(f"Lookup `{field}`: Invalid FtM property.")
                    raise InvalidQuery(f"Lookup `{field}` not any of {meta_fields}")

            if operator:
                if len(operator) > 1:
                    raise InvalidQuery(f"Invalid operator: {operator}")
                operator = operator[0]
                if operator not in self.OPERATORS:
                    raise InvalidQuery(f"Invalid operator: {operator}")

                if operator == "in":
                    if isinstance(value, (str, Query)):
                        value = f"({value})"
                    elif is_listish(value) or isinstance(value, pd.Series):
                        values = ", ".join([f"'{v}'" for v in value])
                        value = f"({values})"
                    else:
                        raise InvalidQuery(f"Invalid value for `IN` operator: {value}")
                elif operator == "null":
                    # field__null=True|False
                    value = "NULL" if as_bool(value) else "NOT NULL"
                else:
                    value = f"'{value}'"
                part = _get_part(field, value, self.OPERATORS[operator])
            else:
                part = _get_part(field, f"'{value}'")

            if field in meta_fields:
                meta_parts.add(part)
            else:
                parts.add(part)

        final_parts = []
        if meta_parts:
            final_parts.append(
                " AND ".join(sorted(meta_parts))
            )  # sort for easier testing
        if parts:
            final_parts.append(" OR ".join(sorted(parts)))  # sort for easier testing
        return " AND ".join(final_parts)

    @property
    def where_part(self) -> str:
        if not self.where_lookup:
            return ""
        return " WHERE " + self._get_lookup_part(self.where_lookup)

    @property
    def having_part(self) -> str:
        if not self.group_part or not self.having_lookup:
            return ""
        return " HAVING " + self._get_lookup_part(self.having_lookup, False)

    @property
    def group_part(self) -> str:
        if self.group_by_fields is None:
            return ""
        return " GROUP BY " + ", ".join(sorted(self.group_by_fields))

    @property
    def order_part(self) -> str:
        if self.order_by_fields is None:
            return ""
        return (
            " ORDER BY " + ", ".join(self.order_by_fields) + " " + self.order_direction
        )

    @property
    def limit_part(self) -> str:
        if self.limit is None and self.offset is None:
            return ""
        offset = self.offset or 0
        if self.limit:
            if self.limit < 0:
                raise InvalidQuery(f"Limit {self.limit} must not be negative")
            return f" LIMIT {offset}, {self.limit}"
        return f" OFFSET {offset}"

    @property
    def is_filtered(self) -> bool:
        return bool(self.where_part or self.having_part)

    def get_query(self) -> str:
        return self.to_str(self)

    def as_table(self) -> pd.DataFrame:
        """pivot via pandas to have columns per prop"""
        df = self.driver.query_dataframe(str(self))
        df = (
            df.groupby(["dataset", "schema", "canonical_id", "prop"])
            .agg({"value": lambda s: s.unique()})
            .reset_index()
        )
        df = df.pivot(
            ["dataset", "canonical_id", "schema"], "prop", "value"
        ).reset_index()
        return df

    @classmethod
    def to_str(cls, query: "Query") -> str:
        rest = "".join(
            (
                query.where_part,
                query.group_part,
                query.having_part,
                query.order_part,
                query.limit_part,
            )
        ).strip()
        q = f"SELECT {query.fields_part} FROM {query.from_} {rest}"
        return q.strip()


class EntityQuery(Query):
    """aggregate rows to entity instances and use where/having/order_by on
    actual properties"""

    fields = ("DISTINCT canonical_id",)

    @property
    def dataset(self):
        if self.where_lookup is not None:
            return self.where_lookup.get("dataset")

    def __iter__(self) -> Iterator[E]:
        res = self.execute()
        entity = None
        for dataset, canonical_id, schema, props, values in res:
            # result is already aggregated per (id, schema) and sorted via query,
            # so each row is 1 entity
            next_entity = model.get_proxy(
                {
                    "id": canonical_id,
                    "schema": schema,
                    "properties": dict(zip(props, values)),
                }
            )
            if entity is None:
                entity = next_entity
            elif entity.id != next_entity.id:
                yield entity
                entity = next_entity
            else:
                entity.merge(next_entity)
        if entity is not None:
            yield entity

    def get_query(self) -> str:
        """
        first get matching canonical_ids for given where clause(s),
        then return 1 row per dataset->canonical_id->schema with props and values
        as arrays

        example:


        SELECT dataset, canonical_id, schema, groupArray(prop) as props, groupArray(values) as values FROM (
            SELECT dataset, canonical_id, schema, prop, groupUniqArray(value) as values FROM ftm
            WHERE canonical_id IN (
                SELECT DISTINCT canonical_id FROM ftm
                WHERE schema = 'Person' AND (prop = 'name' AND value = 'Simon')
            )
            GROUP BY dataset, canonical_id, schema, prop
        )
        GROUP BY dataset, canonical_id, schema
        ORDER BY dataset, canonical_id, schema

        """
        inner = super().get_query()
        outer = (
            Query()
            .select(
                "dataset",
                "canonical_id",
                "schema",
                "prop",
                "groupUniqArray(value) as values",
            )
            .where(canonical_id__in=inner, dataset=self.dataset)
            .group_by("dataset", "canonical_id", "schema", "prop")
        )
        return str(
            Query(outer)
            .select(
                "dataset",
                "canonical_id",
                "schema",
                "groupArray(prop) as props",
                "groupArray(values) as values",
            )
            .group_by("dataset", "canonical_id", "schema")
            .order_by(
                "dataset", "canonical_id", "schema"
            )  # make sure ordering to easier build entities later
        )

    def iterate(self, chunksize: Optional[int] = 1000) -> Iterator[E]:
        # iterate in chunks, useful for huge bulk streaming performance
        q = self
        for i in itertools.count(0, chunksize):
            chunk = (x for x in q[i : i + chunksize])
            try:
                # maybe chunk is empty, then abort
                entity = next(chunk)
                yield entity
            except StopIteration:
                return

            while True:
                try:
                    entity = next(chunk)
                    yield entity
                except StopIteration:
                    break  # to next chunk

    def as_table(self):
        def _unpack(serie: pd.Series) -> pd.Series:
            return pd.Series([i for x in serie.values for i in x]).unique()

        inner = super().get_query()
        q = (
            Query()
            .select(
                "dataset",
                "canonical_id",
                "schema",
                "prop",
                "groupUniqArray(value) as values",
            )
            .where(canonical_id__in=inner)
            .group_by("dataset", "canonical_id", "schema", "prop")
        )
        df = self.driver.query_dataframe(str(q))
        df = (
            df.groupby(["dataset", "schema", "canonical_id", "prop"])
            .agg({"values": _unpack})
            .reset_index()
        )
        df = df.pivot(
            ["dataset", "canonical_id", "schema"], "prop", "values"
        ).reset_index()
        return df
