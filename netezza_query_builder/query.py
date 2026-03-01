"""
Core Query builder for the Netezza SQL dialect.

Netezza limitations vs standard SQL:
  - No RIGHT JOIN or RIGHT OUTER JOIN (rewrite as LEFT JOIN with swapped tables)
  - No FULL OUTER JOIN
  - No WITH RECURSIVE (recursive CTEs are not supported)
  - Identifiers are case-insensitive and stored uppercase internally
  - Schema-qualified table names are common: schema.table

Window function filtering
-------------------------
To filter on a window function result (forbidden in WHERE/HAVING by SQL),
use .where_window(). The query is automatically wrapped in a subquery and
the condition is applied on the outer query:

    Query(Transactions)
        .select(Transactions.id, Transactions.amount)
        .where_window(
            row_number()
                .partition_by(Transactions.account_id)
                .order_by(Transactions.date)
                .alias("rn"),
            "rn = 1",
        )
        .build()

Multiple .where_window() calls are combined into a single subquery level
with AND logic.

CTE usage
---------
Pass a CTE to Query() to register and select from it in one step:

    Query(my_cte).select("*").build()

Or use .with_cte() when the CTE is referenced only in joins/conditions:

    Query(Orders).with_cte(my_cte).join("INNER", my_cte, ...).select("*").build()
"""

from __future__ import annotations

from typing import Literal, Union

from .cte import CTE
from .tables import BaseTable
from .window import WindowFunction

JOIN_TYPE = Literal["LEFT", "LEFT OUTER", "INNER", "CROSS"]

# Alias used for the auto-generated subquery wrapper in where_window.
_SUBQUERY_ALIAS = "_w"

# A table reference can be a BaseTable subclass, a CTE object, or a plain string.
TableRef = Union[type[BaseTable], CTE, str]


def _resolve_table(table: TableRef) -> str:
    """Resolve a TableRef to a SQL table name string."""
    if isinstance(table, type):
        return str(table)  # BaseTable subclass via TableMeta.__str__
    if isinstance(table, CTE):
        return table.name
    return table


def join(join_type: JOIN_TYPE, table: TableRef, condition: str) -> str:
    """
    Build a JOIN clause string.

    Args:
        join_type: One of the supported Netezza join types.
        table:     A BaseTable subclass, CTE object, or plain string table name.
        condition: The ON condition.

    Note: CROSS JOIN does not use an ON condition in Netezza, but passing one
    here will not cause a Python error — Netezza will raise at query execution time.
    """
    resolved = _resolve_table(table)
    if not resolved:
        raise ValueError("Table name cannot be empty")
    if not condition:
        raise ValueError("Join condition cannot be empty")
    return f"{join_type} JOIN {resolved} ON {condition}"


class Query:
    """
    Fluent query builder for Netezza SQL.

    Accepts BaseTable subclasses, CTE objects, or plain strings as table
    references throughout (Query(), .join()). Column attributes from
    BaseTable subclasses are plain strings and work naturally with all
    builder methods. WindowFunction expressions can be passed to .select().
    """

    def __init__(self, table: TableRef) -> None:
        """
        Args:
            table: A BaseTable subclass, a CTE object, or a plain string.
                   - BaseTable subclasses resolve to their schema-qualified name.
                   - CTE objects are registered automatically; Query(my_cte) is
                     equivalent to Query("my_cte").with_cte(my_cte).
                   - Plain strings are used as-is.
        """
        if isinstance(table, CTE):
            self._table = table.name
            self._ctes: list[CTE] = [table]
        else:
            resolved = _resolve_table(table)
            if not resolved:
                raise ValueError("Table name cannot be empty")
            self._table = resolved
            self._ctes = []

        self._columns: list[str] = []
        self._distinct: bool = False
        self._where: str | None = None
        self._joins: list[str] = []
        self._group_by: list[str] = []
        self._having: str | None = None
        self._order_by: list[str] = []
        self._limit: int | None = None
        self._offset: int | None = None

        # Each entry is (WindowFunction, condition_string)
        self._window_conditions: list[tuple[WindowFunction, str]] = []

    # ------------------------------------------------------------------
    # CTE registration
    # ------------------------------------------------------------------

    def with_cte(self, *ctes: CTE) -> Query:
        """
        Add one or more CTEs to the query.

        CTEs are rendered in the order they are added. If a CTE references
        another CTE, the referenced CTE must be added first.

        Prefer passing a CTE directly to Query() when selecting from it —
        use .with_cte() only when the CTE appears in joins or conditions.
        """
        names = [c.name for c in self._ctes]
        for cte in ctes:
            if cte.name in names:
                raise ValueError(f"Duplicate CTE name: '{cte.name}'")
            self._ctes.append(cte)
            names.append(cte.name)
        return self

    # ------------------------------------------------------------------
    # SELECT
    # ------------------------------------------------------------------

    def select(self, *columns: str | WindowFunction) -> Query:
        """
        Args:
            *columns: Column names (strings), BaseTable column attributes,
                      or WindowFunction expressions.
        """
        if not columns:
            raise ValueError("Must select at least one column")
        self._columns.extend(str(c) for c in columns)
        return self

    def distinct(self) -> Query:
        self._distinct = True
        return self

    # ------------------------------------------------------------------
    # WHERE / window filtering
    # ------------------------------------------------------------------

    def where(self, condition: str) -> Query:
        if not condition:
            raise ValueError("WHERE condition cannot be empty")
        self._where = condition
        return self

    def where_window(self, window_func: WindowFunction, condition: str) -> Query:
        """
        Filter on a window function result.

        Because SQL forbids window functions in WHERE clauses, this method
        records the pair. At build time the query is wrapped in a subquery:
        the window function is added to the inner SELECT and the condition
        is applied on the outer WHERE. Multiple calls are AND-ed together.

        The WindowFunction must have an alias (via .alias()), since the outer
        query references the window result by that name.

        Args:
            window_func: A WindowFunction with an alias set.
            condition:   A condition referencing the alias (e.g. "rn = 1").
        """
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

    # ------------------------------------------------------------------
    # JOINs
    # ------------------------------------------------------------------

    def join(self, join_type: JOIN_TYPE, table: TableRef, condition: str) -> Query:
        """
        Args:
            join_type: One of the supported Netezza join types.
            table:     A BaseTable subclass, CTE object, or plain string.
            condition: The ON condition.
        """
        self._joins.append(join(join_type, table, condition))
        return self

    # ------------------------------------------------------------------
    # GROUP BY / HAVING
    # ------------------------------------------------------------------

    def group_by(self, *columns: str) -> Query:
        if not columns:
            raise ValueError("Must provide at least one column to GROUP BY")
        self._group_by.extend(columns)
        return self

    def having(self, condition: str) -> Query:
        if not condition:
            raise ValueError("HAVING condition cannot be empty")
        if not self._group_by:
            raise ValueError("HAVING requires GROUP BY")
        self._having = condition
        return self

    # ------------------------------------------------------------------
    # ORDER BY / LIMIT / OFFSET
    # ------------------------------------------------------------------

    def order_by(self, *columns: str) -> Query:
        if not columns:
            raise ValueError("Must provide at least one column to ORDER BY")
        self._order_by.extend(columns)
        return self

    def limit(self, limit: int) -> Query:
        if limit < 0:
            raise ValueError(f"LIMIT must be a non-negative integer, got {limit}")
        self._limit = limit
        return self

    def offset(self, offset: int) -> Query:
        if offset < 0:
            raise ValueError(f"OFFSET must be a non-negative integer, got {offset}")
        self._offset = offset
        return self

    # ------------------------------------------------------------------
    # Build
    # ------------------------------------------------------------------

    def _build_inner(self) -> str:
        """Build the inner SELECT used when where_window wraps the query.
        Includes all selected columns plus the window function columns needed
        for outer filtering."""
        distinct = "DISTINCT " if self._distinct else ""
        inner_cols = list(self._columns) + [
            str(wf) for wf, _ in self._window_conditions
        ]
        sql = f"SELECT {distinct}{', '.join(inner_cols)} FROM {self._table}"
        if self._joins:
            sql += f" {' '.join(self._joins)}"
        if self._where is not None:
            sql += f" WHERE {self._where}"
        if self._group_by:
            sql += f" GROUP BY {', '.join(self._group_by)}"
        if self._having is not None:
            sql += f" HAVING {self._having}"
        return sql

    def build(self) -> str:
        """Render the query to a SQL string."""
        if not self._columns:
            raise ValueError("Must select at least one column before building")

        cte_prefix = ""
        if self._ctes:
            cte_clauses = ", ".join(cte.build() for cte in self._ctes)
            cte_prefix = f"WITH {cte_clauses} "

        if self._window_conditions:
            inner_sql = self._build_inner()
            outer_where = " AND ".join(cond for _, cond in self._window_conditions)
            sql = (
                f"{cte_prefix}"
                f"SELECT {', '.join(self._columns)} "
                f"FROM ({inner_sql}) AS {_SUBQUERY_ALIAS} "
                f"WHERE {outer_where}"
            )
        else:
            distinct = "DISTINCT " if self._distinct else ""
            sql = f"{cte_prefix}SELECT {distinct}{', '.join(self._columns)} FROM {self._table}"
            if self._joins:
                sql += f" {' '.join(self._joins)}"
            if self._where is not None:
                sql += f" WHERE {self._where}"
            if self._group_by:
                sql += f" GROUP BY {', '.join(self._group_by)}"
            if self._having is not None:
                sql += f" HAVING {self._having}"

        if self._order_by:
            sql += f" ORDER BY {', '.join(self._order_by)}"
        if self._limit is not None:
            sql += f" LIMIT {self._limit}"
        if self._offset is not None:
            sql += f" OFFSET {self._offset}"

        return sql
