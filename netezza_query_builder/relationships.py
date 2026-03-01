"""
Static relationship declarations between BaseTable subclasses.

Relationships are defined after the tables they reference, then stored in a
module-level registry. They serve no runtime SQL purpose — their value is
in documentation, schema visualisation, and IDE discoverability.

Usage
-----
    from netezza_query_builder import relate, get_relationships, graph_data

    relate(
        Transactions, "account_id",
        Accounts,     "id",
        cardinality="many-to-one",
        description="Each transaction belongs to exactly one account.",
    )

Visualisation
-------------
graph_data() returns a plain dict with 'nodes' and 'edges' lists that can be
fed directly into tools such as NetworkX, Graphviz, Mermaid, or any custom
renderer:

    {
        "nodes": [
            {"table": "myschema.Transactions", "description": "...", "columns": [...]},
            ...
        ],
        "edges": [
            {
                "from_table": "myschema.Transactions",
                "from_column": "tx_amount",
                "to_table": "myschema.Accounts",
                "to_column": "acc_id",
                "cardinality": "many-to-one",
                "description": "...",
            },
            ...
        ],
    }
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from .tables import BaseTable

CARDINALITY = Literal["one-to-one", "one-to-many", "many-to-one", "many-to-many"]

_registry: list[Relationship] = []


class Relationship:
    """
    Declares a directed join relationship between two tables.

    Args:
        from_table:       The source BaseTable subclass.
        from_column_attr: The Python attribute name of the FK column on from_table.
        to_table:         The target BaseTable subclass.
        to_column_attr:   The Python attribute name of the PK/UK column on to_table.
        cardinality:      One of "one-to-one", "one-to-many", "many-to-one",
                          "many-to-many".
        description:      Human-readable description of the relationship (optional).
    """

    def __init__(
        self,
        from_table: type[BaseTable],
        from_column_attr: str,
        to_table: type[BaseTable],
        to_column_attr: str,
        cardinality: CARDINALITY = "many-to-one",
        description: str = "",
    ) -> None:
        from .tables import BaseTable as _BaseTable

        for arg, label in ((from_table, "from_table"), (to_table, "to_table")):
            if not (isinstance(arg, type) and issubclass(arg, _BaseTable)):
                raise TypeError(f"{label} must be a BaseTable subclass, got {arg!r}")

        self.from_table = from_table
        self.from_column_attr = from_column_attr
        self.to_table = to_table
        self.to_column_attr = to_column_attr
        self.cardinality = cardinality
        self.description = description

    @property
    def from_column(self) -> str:
        """Physical DB column name on the source table."""
        return self.from_table.raw_column(self.from_column_attr)

    @property
    def to_column(self) -> str:
        """Physical DB column name on the target table."""
        return self.to_table.raw_column(self.to_column_attr)

    def join_condition(self) -> str:
        """Return a fully-qualified SQL ON condition string for this relationship."""
        return (
            f"{getattr(self.from_table, self.from_column_attr)}"
            f" = "
            f"{getattr(self.to_table, self.to_column_attr)}"
        )

    def __repr__(self) -> str:
        return (
            f"Relationship({self.from_table.__name__}.{self.from_column_attr}"
            f" -{self.cardinality}-> "
            f"{self.to_table.__name__}.{self.to_column_attr})"
        )


def relate(
    from_table: type[BaseTable],
    from_column_attr: str,
    to_table: type[BaseTable],
    to_column_attr: str,
    cardinality: CARDINALITY = "many-to-one",
    description: str = "",
) -> Relationship:
    """
    Declare a relationship and add it to the global registry.

    Returns the created Relationship so it can be stored locally if needed.
    """
    rel = Relationship(
        from_table, from_column_attr,
        to_table,   to_column_attr,
        cardinality=cardinality,
        description=description,
    )
    _registry.append(rel)
    return rel


def get_relationships(
    table: type[BaseTable] | None = None,
) -> list[Relationship]:
    """
    Return registered relationships, optionally filtered by table.

    Args:
        table: If given, returns only relationships where this table appears
               as the from_table OR to_table.
    """
    if table is None:
        return list(_registry)
    return [
        r for r in _registry
        if r.from_table is table or r.to_table is table
    ]


def graph_data() -> dict:
    """
    Return a plain-dict representation of all registered tables and
    relationships, suitable for graph visualisation tools.

    Nodes are deduplicated across relationships. Every table that appears
    in at least one relationship becomes a node; isolated tables (those
    with no declared relationships) are omitted.

    Returns:
        {
            "nodes": [
                {
                    "table":       "<schema>.<TableName>",
                    "description": "<table description>",
                    "columns": [
                        {
                            "attr":        "<Python attr name>",
                            "name":        "<DB column name>",
                            "description": "<column description>",
                        },
                        ...
                    ],
                },
                ...
            ],
            "edges": [
                {
                    "from_table":   "<schema>.<TableName>",
                    "from_column":  "<DB column name>",
                    "to_table":     "<schema>.<TableName>",
                    "to_column":    "<DB column name>",
                    "cardinality":  "<cardinality>",
                    "description":  "<relationship description>",
                },
                ...
            ],
        }
    """
    seen_tables: dict[str, type[BaseTable]] = {}
    for rel in _registry:
        for tbl in (rel.from_table, rel.to_table):
            key = str(tbl)
            if key not in seen_tables:
                seen_tables[key] = tbl

    nodes = []
    for key, tbl in seen_tables.items():
        nodes.append({
            "table":       key,
            "description": getattr(tbl, "__description__", ""),
            "columns": [
                {
                    "attr":        attr,
                    "name":        meta.name,
                    "description": meta.description,
                }
                for attr, meta in tbl.columns().items()
            ],
        })

    edges = [
        {
            "from_table":  str(r.from_table),
            "from_column": r.from_column,
            "to_table":    str(r.to_table),
            "to_column":   r.to_column,
            "cardinality": r.cardinality,
            "description": r.description,
        }
        for r in _registry
    ]

    return {"nodes": nodes, "edges": edges}
