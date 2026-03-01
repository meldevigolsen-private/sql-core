"""
netezza_query_builder
~~~~~~~~~~~~~~~~~~~~~
Fluent SQL query builder targeting the Netezza dialect.

Quick start:
    from netezza_query_builder import BaseTable, Column, Query, CTE, row_number, relate

    class Users(BaseTable):
        __schema__      = "myschema"
        __description__ = "Registered users."
        id     = Column("user_id", description="Primary key.")
        active = Column("is_active")

    sql = Query(Users).select(Users.id).where(f"{Users.active} = true").build()
"""

from .column import Column
from .cte import CTE
from .query import JOIN_TYPE, Query, join
from .relationships import (
    CARDINALITY,
    Relationship,
    get_relationships,
    graph_data,
    relate,
)
from .tables import BaseTable, TableMeta
from .window import (
    WindowFunction,
    avg_over,
    count_over,
    dense_rank,
    first_value,
    lag,
    last_value,
    lead,
    max_over,
    min_over,
    rank,
    row_number,
    sum_over,
)

__all__ = [
    # Columns
    "Column",
    # Tables
    "BaseTable",
    "TableMeta",
    # Query
    "Query",
    "join",
    "JOIN_TYPE",
    # CTE
    "CTE",
    # Relationships
    "Relationship",
    "relate",
    "get_relationships",
    "graph_data",
    "CARDINALITY",
    # Window functions
    "WindowFunction",
    "row_number",
    "rank",
    "dense_rank",
    "lag",
    "lead",
    "first_value",
    "last_value",
    "sum_over",
    "count_over",
    "avg_over",
    "min_over",
    "max_over",
]
