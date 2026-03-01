"""
SQL query builder targeting the Netezza dialect.

Netezza limitations vs standard SQL:
  - No RIGHT JOIN or RIGHT OUTER JOIN (rewrite as LEFT JOIN with swapped tables)
  - No FULL OUTER JOIN
  - No WITH RECURSIVE (Netezza does not support recursive CTEs)
  - Identifiers are case-insensitive and stored uppercase internally
  - Schema-qualified table names are common: schema.table

Table definitions use BaseTable subclasses, which resolve to their schema-qualified
name automatically when used in Query and CTE builders.
"""

from typing import Literal

JOIN_TYPE = Literal["LEFT", "LEFT OUTER", "INNER", "CROSS"]

# Type alias for anything accepted as a table reference
TableRef = type["BaseTable"] | str


# ---------------------------------------------------------------------------
# Table metadata layer
# ---------------------------------------------------------------------------


class TableMeta(type):
    """
    Metaclass that makes a BaseTable subclass behave like its table name
    when used in f-strings or str() calls.

    If the subclass defines __schema__, the string representation is
    'schema.tablename', otherwise just 'tablename'.

    Example:
        class Transactions(BaseTable):
            __schema__ = "myschema"
            id = "tx_id"
            amount = "tx_amount"

        f"{Transactions}"         # → "myschema.transactions"
        Transactions.amount       # → "tx_amount"
    """

    def __str__(cls) -> str:
        name = cls.__name__.lower()
        schema = getattr(cls, "__schema__", None)
        return f"{schema}.{name}" if schema else name

    @property
    def table_name(cls) -> str:
        return str(cls)


class BaseTable(metaclass=TableMeta):
    """
    Base class for table definitions. Subclass this to define your tables.

    Class attributes represent column names as they exist in the database.
    Set __schema__ to automatically qualify the table name with a schema.

    Example:
        class Accounts(BaseTable):
            __schema__ = "myschema"
            id = "acc_id"
            balance = "current_balance"
    """

    __schema__: str | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_table(table: TableRef) -> str:
    """Resolve a TableRef (BaseTable subclass or plain string) to a SQL table name."""
    return str(table) if isinstance(table, type) else table


def join(
    join_type: JOIN_TYPE,
    table: TableRef,
    condition: str,
) -> str:
    """
    Build a JOIN clause string.

    Args:
        join_type: One of the supported Netezza join types.
        table: A BaseTable subclass or a plain string table name.
        condition: The ON condition.

    Note: CROSS JOIN does not use an ON condition in Netezza, but passing
    one here will not cause a syntax error — Netezza will raise at query time.
    """
    resolved = _resolve_table(table)
    if not resolved:
        raise ValueError("Table name cannot be empty")
    if not condition:
        raise ValueError("Join condition cannot be empty")
    return f"{join_type} JOIN {resolved} ON {condition}"


# ---------------------------------------------------------------------------
# CTE
# ---------------------------------------------------------------------------


class CTE:
    """
    Represents a reusable Common Table Expression (WITH clause).

    Defined once from a Query object and can be passed to any number of
    Query instances via .with_cte(). The same CTE instance can be shared
    across multiple queries without redefining it.

    Example:
        active_users = CTE(
            "active_users",
            Query(Users).select(Users.id, Users.name).where(f"{Users.active} = true"),
        )

        query1 = Query("active_users").with_cte(active_users).select("*").build()
        query2 = (
            Query("active_users")
            .with_cte(active_users)
            .select(Users.id)
            .where(f"{Users.name} LIKE 'A%'")
            .build()
        )
    """

    def __init__(self, name: str, query: "Query") -> None:
        if not name:
            raise ValueError("CTE name cannot be empty")
        if not isinstance(query, Query):
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


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------


class Query:
    """
    Fluent query builder for Netezza SQL.

    Accepts BaseTable subclasses or plain strings as table references.
    Column attributes from BaseTable subclasses (e.g. Transactions.amount)
    are plain strings and work naturally with all builder methods.

    Example:
        class Transactions(BaseTable):
            __schema__ = "myschema"
            id = "tx_id"
            amount = "tx_amount"
            status = "tx_status"

        class Accounts(BaseTable):
            __schema__ = "myschema"
            id = "acc_id"
            balance = "current_balance"

        query = (
            Query(Transactions)
            .select(Transactions.amount, Transactions.status, Accounts.balance)
            .join("INNER", Accounts, f"{Transactions.id} = {Accounts.id}")
            .where(f"{Transactions.status} = 'active'")
            .order_by(Transactions.amount)
            .limit(100)
            .build()
        )
    """

    def __init__(self, table: TableRef) -> None:
        """
        Args:
            table: A BaseTable subclass or a plain string. BaseTable subclasses
                   automatically resolve to their schema-qualified name.
                   Pass a plain string when selecting from a CTE by name.
        """
        resolved = _resolve_table(table)
        if not resolved:
            raise ValueError("Table name cannot be empty")
        self._table = resolved

        self._ctes: list[CTE] = []
        self._columns: list[str] = []
        self._distinct: bool = False
        self._where: str | None = None
        self._joins: list[str] = []
        self._group_by: list[str] = []
        self._having: str | None = None
        self._order_by: list[str] = []
        self._limit: int | None = None
        self._offset: int | None = None

    def with_cte(self, *ctes: CTE) -> "Query":
        """
        Add one or more CTEs to the query.

        CTEs are rendered in the order they are added. If a CTE references
        another CTE, the referenced CTE must be added first.

        Since CTE objects are defined independently, the same CTE instance
        can be passed to multiple Query objects without any side effects.
        """
        names = [c.name for c in self._ctes]
        for cte in ctes:
            if cte.name in names:
                raise ValueError(f"Duplicate CTE name: '{cte.name}'")
            self._ctes.append(cte)
            names.append(cte.name)
        return self

    def select(self, *columns: str) -> "Query":
        if not columns:
            raise ValueError("Must select at least one column")
        self._columns.extend(columns)
        return self

    def distinct(self) -> "Query":
        self._distinct = True
        return self

    def where(self, condition: str) -> "Query":
        if not condition:
            raise ValueError("WHERE condition cannot be empty")
        self._where = condition
        return self

    def join(
        self,
        join_type: JOIN_TYPE,
        table: TableRef,
        condition: str,
    ) -> "Query":
        """
        Args:
            join_type: One of the supported Netezza join types.
            table: A BaseTable subclass or a plain string (including CTE names).
            condition: The ON condition.
        """
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

    def build(self) -> str:
        if not self._columns:
            raise ValueError("Must select at least one column before building")

        query = ""

        if self._ctes:
            cte_clauses = ", ".join(cte.build() for cte in self._ctes)
            query = f"WITH {cte_clauses} "

        distinct = "DISTINCT " if self._distinct else ""
        query += f"SELECT {distinct}{', '.join(self._columns)} FROM {self._table}"

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


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # --- Table definitions ---

    class Transactions(BaseTable):
        __schema__ = "myschema"
        id = "tx_id"
        amount = "tx_amount"
        status = "tx_status"

    class Accounts(BaseTable):
        __schema__ = "myschema"
        id = "acc_id"
        balance = "current_balance"

    # Example 1: plain join using BaseTable references
    query1 = (
        Query(Transactions)
        .select(Transactions.amount, Transactions.status, Accounts.balance)
        .join("INNER", Accounts, f"{Transactions.id} = {Accounts.id}")
        .where(f"{Transactions.status} = 'active'")
        .order_by(Transactions.amount)
        .limit(100)
        .build()
    )
    print("Example 1:")
    print(query1)
    print()

    # Example 2: CTEs with BaseTable references, reused across two queries
    active_txns = CTE(
        "active_txns",
        Query(Transactions)
        .select(Transactions.id, Transactions.amount)
        .where(f"{Transactions.status} = 'active'"),
    )

    query2 = (
        Query("active_txns")
        .with_cte(active_txns)
        .select(Transactions.id, Transactions.amount, Accounts.balance)
        .join("LEFT", Accounts, f"{Transactions.id} = {Accounts.id}")
        .order_by(Transactions.amount)
        .build()
    )
    print("Example 2 (with CTE):")
    print(query2)
    print()

    # Reuse active_txns CTE in a different query
    query3 = (
        Query("active_txns")
        .with_cte(active_txns)
        .select(Transactions.id)
        .where(f"{Transactions.amount} > 1000")
        .build()
    )
    print("Example 3 (reusing CTE):")
    print(query3)
