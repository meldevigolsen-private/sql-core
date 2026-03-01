"""
Query builder for Netezza SQL.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Tuple, Union

from cte import CTE
from helpers import _SUBQUERY_ALIAS, _resolve_table, join
from window import WindowFunction

if TYPE_CHECKING:
    from tables import BaseTable


class Query:
    def __init__(self, table: "Union[type[BaseTable], CTE, str]") -> None:
        # Register CTE automatically when it's passed as the primary table
        if isinstance(table, CTE):
            self._table = table.name
            self._ctes: List[CTE] = [table]
        else:
            resolved = _resolve_table(table)
            if not resolved:
                raise ValueError("Table name cannot be empty")
            self._table = resolved
            self._ctes = []

        self._columns: List[str] = []
        self._distinct: bool = False
        self._where: str | None = None
        self._joins: List[str] = []
        self._group_by: List[str] = []
        self._having: str | None = None
        self._order_by: List[str] = []
        self._limit: int | None = None
        self._offset: int | None = None

        # Each entry is (WindowFunction, condition_string)
        self._window_conditions: List[Tuple[WindowFunction, str]] = []

    def with_cte(self, *ctes: CTE) -> "Query":
        names = [c.name for c in self._ctes]
        for cte in ctes:
            if cte.name in names:
                raise ValueError(f"Duplicate CTE name: '{cte.name}'")
            self._ctes.append(cte)
            names.append(cte.name)
        return self

    def select(self, *columns: str | WindowFunction) -> "Query":
        if not columns:
            raise ValueError("Must select at least one column")
        self._columns.extend(str(c) for c in columns)
        return self

    def distinct(self) -> "Query":
        self._distinct = True
        return self

    def where(self, condition: str) -> "Query":
        if not condition:
            raise ValueError("WHERE condition cannot be empty")
        self._where = condition
        return self

    def where_window(self, window_func: WindowFunction, condition: str) -> "Query":
        if not isinstance(window_func, WindowFunction):
            raise TypeError("where_window requires a WindowFunction instance")
        if not window_func._alias:
            raise ValueError(
                "WindowFunction passed to where_window must have an alias set via .alias()"
            )
        if not condition:
            raise ValueError("where_window condition cannot be empty")
        self._window_conditions.append((window_func, condition))
        return self

    def join(
        self, join_type: str, table: "Union[type[BaseTable], CTE, str]", condition: str
    ) -> "Query":
        self._joins.append(join(join_type, table, condition))
        return self

    def group_by(self, *columns: str) -> "Query":
        if not columns:
            raise ValueError("Must provide at least one column to GROUP BY")
        self._group_by.extend(columns)
        return self

    def having(self, condition: str) -> "Query":
        if not condition:
            raise ValueError("HAVING condition cannot be empty")
        if not self._group_by:
            raise ValueError("HAVING requires GROUP BY")
        self._having = condition
        return self

    def order_by(self, *columns: str) -> "Query":
        if not columns:
            raise ValueError("Must provide at least one column to ORDER BY")
        self._order_by.extend(columns)
        return self

    def limit(self, limit: int) -> "Query":
        if limit < 0:
            raise ValueError(f"LIMIT must be a non-negative integer, got {limit}")
        self._limit = limit
        return self

    def offset(self, offset: int) -> "Query":
        if offset < 0:
            raise ValueError(f"OFFSET must be a non-negative integer, got {offset}")
        self._offset = offset
        return self

    def _build_inner(self) -> str:
        distinct = "DISTINCT " if self._distinct else ""
        inner_cols = list(self._columns) + [
            str(wf) for wf, _ in self._window_conditions
        ]
        query = f"SELECT {distinct}{', '.join(inner_cols)} FROM {self._table}"
        if self._joins:
            query += f" {' '.join(self._joins)}"
        if self._where is not None:
            query += f" WHERE {self._where}"
        if self._group_by:
            query += f" GROUP BY {', '.join(self._group_by)}"
        if self._having is not None:
            query += f" HAVING {self._having}"
        return query

    def build(self) -> str:
        if not self._columns:
            raise ValueError("Must select at least one column before building")

        cte_prefix = ""
        if self._ctes:
            cte_clauses = ", ".join(cte.build() for cte in self._ctes)
            cte_prefix = f"WITH {cte_clauses} "

        if self._window_conditions:
            inner_sql = self._build_inner()
            outer_where = " AND ".join(cond for _, cond in self._window_conditions)
            query = (
                f"{cte_prefix}"
                f"SELECT {', '.join(self._columns)} "
                f"FROM ({inner_sql}) AS {_SUBQUERY_ALIAS} "
                f"WHERE {outer_where}"
            )
        else:
            distinct = "DISTINCT " if self._distinct else ""
            query = f"{cte_prefix}SELECT {distinct}{', '.join(self._columns)} FROM {self._table}"
            if self._joins:
                query += f" {' '.join(self._joins)}"
            if self._where is not None:
                query += f" WHERE {self._where}"
            if self._group_by:
                query += f" GROUP BY {', '.join(self._group_by)}"
            if self._having is not None:
                query += f" HAVING {self._having}"

        if self._order_by:
            query += f" ORDER BY {', '.join(self._order_by)}"
        if self._limit is not None:
            query += f" LIMIT {self._limit}"
        if self._offset is not None:
            query += f" OFFSET {self._offset}"

        return query
