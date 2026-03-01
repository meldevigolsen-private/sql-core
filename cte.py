"""
CTE (Common Table Expression) representation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from query import Query


class CTE:
    def __init__(self, name: str, query: "Query") -> None:
        if not name:
            raise ValueError("CTE name cannot be empty")
        # Query type is only imported for type checking to avoid circular imports

        self._name = name
        self._query = query

    @property
    def name(self) -> str:
        return self._name

    def build(self) -> str:
        return f"{self._name} AS ({self._query.build()})"
