"""
Table metadata layer: `TableMeta` and `BaseTable`.
"""

from __future__ import annotations


class TableMeta(type):
    def __str__(cls) -> str:
        name = cls.__name__
        schema = getattr(cls, "__schema__", None)
        return f"{schema}.{name}" if schema else name

    @property
    def table_name(cls) -> str:
        return str(cls)


class BaseTable(metaclass=TableMeta):
    """
    Base class for table definitions. Subclass to define tables.
    Class attributes represent column names as they exist in the database.
    """

    __schema__: str | None = None
