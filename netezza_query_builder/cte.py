"""
Common Table Expression (WITH clause) support for the Netezza query builder.

Usage
-----
Pass a CTE directly to Query() to register it and select from it in one step
(preferred):

    my_cte = CTE("active_users", Query(Users).select("*").where("active = true"))
    Query(my_cte).select("*").build()

Or register manually via .with_cte() when the CTE is used in a join rather
than as the primary FROM table:

    Query(Orders).with_cte(my_cte).select("*").join("INNER", my_cte, ...).build()

Note: Netezza does not support WITH RECURSIVE.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .query import Query


class CTE:
    """
    Represents a reusable Common Table Expression (WITH clause).

    Defined once from a Query object and can be referenced by any number of
    subsequent Query instances.
    """

    def __init__(self, name: str, query: Query) -> None:
        # Import here to avoid circular import at module load time.
        from .query import Query as _Query

        if not name:
            raise ValueError("CTE name cannot be empty")
        if not isinstance(query, _Query):
            raise TypeError(
                f"CTE query must be a Query object, got {type(query).__name__}"
            )
        self._name = name
        self._query = query

    @property
    def name(self) -> str:
        return self._name

    def build(self) -> str:
        return f"{self._name} AS ({self._query.build()})"
