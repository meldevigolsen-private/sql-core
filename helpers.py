"""
Helper utilities: table resolution and join building.
"""

from __future__ import annotations

from typing import Union

from cte import CTE
from tables import BaseTable

JOIN_TYPE = "LEFT", "LEFT OUTER", "INNER", "CROSS"


# A table reference can be a BaseTable subclass, a CTE object, or a plain string.
TableRef = Union[type[BaseTable], CTE, str]


_SUBQUERY_ALIAS = "_w"


def _resolve_table(table: TableRef) -> str:
    if isinstance(table, type):
        return str(table)
    if isinstance(table, CTE):
        return table.name
    return table


def join(join_type: str, table: TableRef, condition: str) -> str:
    resolved = _resolve_table(table)
    if not resolved:
        raise ValueError("Table name cannot be empty")
    if not condition:
        raise ValueError("Join condition cannot be empty")
    return f"{join_type} JOIN {resolved} ON {condition}"
