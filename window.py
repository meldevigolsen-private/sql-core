"""
Window function builder and named helpers.
"""

from __future__ import annotations

from typing import List


class WindowFunction:
    def __init__(self, func: str) -> None:
        self._func = func
        self._partition_by: List[str] = []
        self._order_by: List[str] = []
        self._alias: str | None = None

    def partition_by(self, *columns: str) -> "WindowFunction":
        if not columns:
            raise ValueError("partition_by requires at least one column")
        self._partition_by.extend(columns)
        return self

    def order_by(self, *columns: str) -> "WindowFunction":
        if not columns:
            raise ValueError("order_by requires at least one column")
        self._order_by.extend(columns)
        return self

    def alias(self, name: str) -> "WindowFunction":
        if not name:
            raise ValueError("Alias cannot be empty")
        self._alias = name
        return self

    def build(self) -> str:
        over_clauses: List[str] = []
        if self._partition_by:
            over_clauses.append(f"PARTITION BY {', '.join(self._partition_by)}")
        if self._order_by:
            over_clauses.append(f"ORDER BY {', '.join(self._order_by)}")
        over = f"OVER ({' '.join(over_clauses)})"
        expr = f"{self._func} {over}"
        return f"{expr} AS {self._alias}" if self._alias else expr

    def __str__(self) -> str:
        return self.build()


# Named helpers
def row_number() -> WindowFunction:
    return WindowFunction("ROW_NUMBER()")


def rank() -> WindowFunction:
    return WindowFunction("RANK()")


def dense_rank() -> WindowFunction:
    return WindowFunction("DENSE_RANK()")


def lag(column: str, offset: int = 1, default: str | None = None) -> WindowFunction:
    args = [column, str(offset)]
    if default is not None:
        args.append(default)
    return WindowFunction(f"LAG({', '.join(args)})")


def lead(column: str, offset: int = 1, default: str | None = None) -> WindowFunction:
    args = [column, str(offset)]
    if default is not None:
        args.append(default)
    return WindowFunction(f"LEAD({', '.join(args)})")


def first_value(column: str) -> WindowFunction:
    return WindowFunction(f"FIRST_VALUE({column})")


def last_value(column: str) -> WindowFunction:
    return WindowFunction(f"LAST_VALUE({column})")


def sum_over(column: str) -> WindowFunction:
    return WindowFunction(f"SUM({column})")


def count_over(column: str = "*") -> WindowFunction:
    return WindowFunction(f"COUNT({column})")


def avg_over(column: str) -> WindowFunction:
    return WindowFunction(f"AVG({column})")


def min_over(column: str) -> WindowFunction:
    return WindowFunction(f"MIN({column})")


def max_over(column: str) -> WindowFunction:
    return WindowFunction(f"MAX({column})")
