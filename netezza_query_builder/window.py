"""
Window function expressions for the Netezza query builder.

WindowFunction can be passed to Query.select() directly, or to
Query.where_window() to filter on the result (which requires an alias).

Named helpers
-------------
    row_number()          ROW_NUMBER()
    rank()                RANK()
    dense_rank()          DENSE_RANK()
    lag(col, offset)      LAG(col, offset)
    lead(col, offset)     LEAD(col, offset)
    first_value(col)      FIRST_VALUE(col)
    last_value(col)       LAST_VALUE(col)
    sum_over(col)         SUM(col) as window
    count_over(col)       COUNT(col) as window
    avg_over(col)         AVG(col) as window
    min_over(col)         MIN(col) as window
    max_over(col)         MAX(col) as window

Chaining:
    .partition_by(*cols)  PARTITION BY clause (optional)
    .order_by(*cols)      ORDER BY clause
    .alias(name)          AS alias — required for where_window

Example:
    row_number().partition_by(Transactions.account_id).order_by(Transactions.date).alias("rn")
    # -> "ROW_NUMBER() OVER (PARTITION BY acc_id ORDER BY tx_date) AS rn"
"""

from __future__ import annotations


class WindowFunction:
    """
    Builds a SQL window function expression.

    Construct via the named helpers below rather than instantiating directly.
    """

    def __init__(self, func: str) -> None:
        self._func = func
        self._partition_by: list[str] = []
        self._order_by: list[str] = []
        self._alias: str | None = None

    def partition_by(self, *columns: str) -> WindowFunction:
        if not columns:
            raise ValueError("partition_by requires at least one column")
        self._partition_by.extend(columns)
        return self

    def order_by(self, *columns: str) -> WindowFunction:
        if not columns:
            raise ValueError("order_by requires at least one column")
        self._order_by.extend(columns)
        return self

    def alias(self, name: str) -> WindowFunction:
        if not name:
            raise ValueError("Alias cannot be empty")
        self._alias = name
        return self

    def build(self) -> str:
        over_clauses: list[str] = []
        if self._partition_by:
            over_clauses.append(f"PARTITION BY {', '.join(self._partition_by)}")
        if self._order_by:
            over_clauses.append(f"ORDER BY {', '.join(self._order_by)}")
        over = f"OVER ({' '.join(over_clauses)})"
        expr = f"{self._func} {over}"
        return f"{expr} AS {self._alias}" if self._alias else expr

    def __str__(self) -> str:
        return self.build()


# ---------------------------------------------------------------------------
# Named helpers
# ---------------------------------------------------------------------------

def row_number() -> WindowFunction:
    """ROW_NUMBER() - unique sequential integer per partition."""
    return WindowFunction("ROW_NUMBER()")


def rank() -> WindowFunction:
    """RANK() - rank with gaps for ties."""
    return WindowFunction("RANK()")


def dense_rank() -> WindowFunction:
    """DENSE_RANK() - rank without gaps for ties."""
    return WindowFunction("DENSE_RANK()")


def lag(column: str, offset: int = 1, default: str | None = None) -> WindowFunction:
    """LAG(column, offset, default) - value from a preceding row."""
    args = [column, str(offset)]
    if default is not None:
        args.append(default)
    return WindowFunction(f"LAG({', '.join(args)})")


def lead(column: str, offset: int = 1, default: str | None = None) -> WindowFunction:
    """LEAD(column, offset, default) - value from a following row."""
    args = [column, str(offset)]
    if default is not None:
        args.append(default)
    return WindowFunction(f"LEAD({', '.join(args)})")


def first_value(column: str) -> WindowFunction:
    """FIRST_VALUE(column) - first value in the window frame."""
    return WindowFunction(f"FIRST_VALUE({column})")


def last_value(column: str) -> WindowFunction:
    """LAST_VALUE(column) - last value in the window frame."""
    return WindowFunction(f"LAST_VALUE({column})")


def sum_over(column: str) -> WindowFunction:
    """SUM(column) as a window function."""
    return WindowFunction(f"SUM({column})")


def count_over(column: str = "*") -> WindowFunction:
    """COUNT(column) as a window function. Defaults to COUNT(*)."""
    return WindowFunction(f"COUNT({column})")


def avg_over(column: str) -> WindowFunction:
    """AVG(column) as a window function."""
    return WindowFunction(f"AVG({column})")


def min_over(column: str) -> WindowFunction:
    """MIN(column) as a window function."""
    return WindowFunction(f"MIN({column})")


def max_over(column: str) -> WindowFunction:
    """MAX(column) as a window function."""
    return WindowFunction(f"MAX({column})")
