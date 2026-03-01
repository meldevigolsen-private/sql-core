"""
SQL query builder targeting the Netezza dialect.

Netezza limitations vs standard SQL:
  - No RIGHT JOIN or RIGHT OUTER JOIN (rewrite as LEFT JOIN with swapped tables)
  - No FULL OUTER JOIN
  - No WITH RECURSIVE (Netezza does not support recursive CTEs)
  - Identifiers are case-insensitive and stored uppercase internally
  - Schema-qualified table names are common: schema.table

Window functions:
  Window functions can be used in .select() directly. To filter on a window
  function result (which SQL forbids in WHERE/HAVING), use .where_window():

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

  This automatically wraps the query in a subquery, adds the window function
  column to the inner SELECT, and applies the condition on the outer query.
  Multiple .where_window() calls are combined into a single subquery level
  with AND logic.
"""

from typing import Literal

JOIN_TYPE = Literal["LEFT", "LEFT OUTER", "INNER", "CROSS"]
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

        f"{Transactions}"      # → "myschema.transactions"
        Transactions.amount    # → "tx_amount"
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
# Window functions
# ---------------------------------------------------------------------------


class WindowFunction:
    """
    Builds a SQL window function expression for use in Query.select()
    or Query.where_window().

    Construct via the named helpers (row_number, rank, dense_rank, etc.)
    rather than instantiating directly.

    Chaining:
        .partition_by(*cols)  — PARTITION BY clause (optional)
        .order_by(*cols)      — ORDER BY clause (required for ROW_NUMBER,
                                RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE,
                                LAST_VALUE; optional for aggregates)
        .alias(name)          — AS alias (required when used in where_window)

    Example:
        row_number().partition_by(Transactions.account_id).order_by(Transactions.date).alias("rn")
        # → "ROW_NUMBER() OVER (PARTITION BY acc_id ORDER BY tx_date) AS rn"

        lag(Transactions.amount, 1).partition_by(Transactions.account_id).order_by(Transactions.date)
        # → "LAG(tx_amount, 1) OVER (PARTITION BY acc_id ORDER BY tx_date)"
    """

    def __init__(self, func: str) -> None:
        self._func = func
        self._partition_by: list[str] = []
        self._order_by: list[str] = []
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


# --- Named window function helpers ---


def row_number() -> WindowFunction:
    """ROW_NUMBER() — unique sequential integer per partition."""
    return WindowFunction("ROW_NUMBER()")


def rank() -> WindowFunction:
    """RANK() — rank with gaps for ties."""
    return WindowFunction("RANK()")


def dense_rank() -> WindowFunction:
    """DENSE_RANK() — rank without gaps for ties."""
    return WindowFunction("DENSE_RANK()")


def lag(column: str, offset: int = 1, default: str | None = None) -> WindowFunction:
    """LAG(column, offset, default) — value from a preceding row."""
    args = [column, str(offset)]
    if default is not None:
        args.append(default)
    return WindowFunction(f"LAG({', '.join(args)})")


def lead(column: str, offset: int = 1, default: str | None = None) -> WindowFunction:
    """LEAD(column, offset, default) — value from a following row."""
    args = [column, str(offset)]
    if default is not None:
        args.append(default)
    return WindowFunction(f"LEAD({', '.join(args)})")


def first_value(column: str) -> WindowFunction:
    """FIRST_VALUE(column) — first value in the window frame."""
    return WindowFunction(f"FIRST_VALUE({column})")


def last_value(column: str) -> WindowFunction:
    """LAST_VALUE(column) — last value in the window frame."""
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SUBQUERY_ALIAS = "_w"


def _resolve_table(table: TableRef) -> str:
    """Resolve a TableRef (BaseTable subclass or plain string) to a SQL table name."""
    return str(table) if isinstance(table, type) else table


def join(join_type: JOIN_TYPE, table: TableRef, condition: str) -> str:
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

    For filtering on window function results, prefer .where_window() on
    the Query directly — it handles the subquery wrapping automatically.
    Only use CTE manually when you need to reference the windowed result
    in multiple subsequent queries.
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
    Column attributes from BaseTable subclasses are plain strings and work
    naturally with all builder methods. WindowFunction expressions can be
    passed directly to .select().

    To filter on a window function result use .where_window() — it
    automatically wraps the query in a subquery so the condition can be
    applied legally. Multiple .where_window() calls are combined into a
    single subquery level and AND-ed together.

    Example:
        # Filtering on a window function — subquery generated automatically
        query = (
            Query(Transactions)
            .select(Transactions.id, Transactions.amount)
            .where_window(
                row_number()
                    .partition_by(Transactions.account_id)
                    .order_by(f"{Transactions.date} DESC")
                    .alias("rn"),
                "rn = 1",
            )
            .build()
        )

        # Window function in SELECT only, no filtering needed
        query = (
            Query(Transactions)
            .select(
                Transactions.id,
                Transactions.amount,
                rank()
                    .partition_by(Transactions.account_id)
                    .order_by(f"{Transactions.amount} DESC")
                    .alias("rnk"),
            )
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

        # Each entry is (WindowFunction, condition_string)
        self._window_conditions: list[tuple[WindowFunction, str]] = []

    def with_cte(self, *ctes: CTE) -> "Query":
        """
        Add one or more CTEs to the query.

        CTEs are rendered in the order they are added. If a CTE references
        another CTE, the referenced CTE must be added first.
        """
        names = [c.name for c in self._ctes]
        for cte in ctes:
            if cte.name in names:
                raise ValueError(f"Duplicate CTE name: '{cte.name}'")
            self._ctes.append(cte)
            names.append(cte.name)
        return self

    def select(self, *columns: str | WindowFunction) -> "Query":
        """
        Args:
            *columns: Column names (strings), BaseTable column attributes,
                      or WindowFunction expressions.
        """
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
        """
        Filter on a window function result.

        Because SQL forbids window functions in WHERE clauses, this method
        records the window function and condition pair. At build time, the
        query is automatically wrapped in a subquery: the window function is
        added to the inner SELECT and the condition is applied in the outer WHERE.

        Multiple calls are combined into a single subquery level, with
        conditions joined by AND.

        The WindowFunction must have an alias set (via .alias()), since the
        outer query references the window result by that name.

        Args:
            window_func: A WindowFunction with an alias assigned.
            condition:   A WHERE condition referencing the window alias
                         (e.g. "rn = 1" or "running_total > 1000").

        Example:
            .where_window(
                row_number()
                    .partition_by(Transactions.account_id)
                    .order_by(Transactions.date)
                    .alias("rn"),
                "rn = 1",
            )
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

    def join(self, join_type: JOIN_TYPE, table: TableRef, condition: str) -> "Query":
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

    def _build_inner(self) -> str:
        """Build the raw SELECT...FROM...JOIN...WHERE...GROUP BY...HAVING string
        without window-condition wrapping, CTEs, ORDER BY, LIMIT, or OFFSET.
        Used internally to construct the inner subquery when where_window is used."""
        distinct = "DISTINCT " if self._distinct else ""

        # Inner query includes all selected columns + window function columns
        inner_cols = list(self._columns)
        for wf, _ in self._window_conditions:
            inner_cols.append(str(wf))

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
            # Wrap the inner query as a subquery; apply window conditions in outer WHERE
            inner_sql = self._build_inner()
            outer_where = " AND ".join(cond for _, cond in self._window_conditions)

            # Outer SELECT only exposes the originally requested columns
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


# ---------------------------------------------------------------------------
# Example usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":

    class Transactions(BaseTable):
        __schema__ = "myschema"
        id = "tx_id"
        account_id = "acc_id"
        amount = "tx_amount"
        status = "tx_status"
        date = "tx_date"

    class Accounts(BaseTable):
        __schema__ = "myschema"
        id = "acc_id"
        balance = "current_balance"

    # Example 1: latest transaction per account using where_window
    # — subquery wrapper is generated automatically
    query1 = (
        Query(Transactions)
        .select(Transactions.id, Transactions.account_id, Transactions.amount)
        .where(f"{Transactions.status} = 'active'")
        .where_window(
            row_number()
            .partition_by(Transactions.account_id)
            .order_by(f"{Transactions.date} DESC")
            .alias("rn"),
            "rn = 1",
        )
        .order_by(Transactions.account_id)
        .build()
    )
    print("Example 1 (where_window — latest transaction per account):")
    print(query1)
    print()

    # Example 2: multiple where_window conditions combined into one subquery
    query2 = (
        Query(Transactions)
        .select(Transactions.id, Transactions.account_id, Transactions.amount)
        .where_window(
            row_number()
            .partition_by(Transactions.account_id)
            .order_by(f"{Transactions.date} DESC")
            .alias("rn"),
            "rn <= 3",
        )
        .where_window(
            sum_over(Transactions.amount)
            .partition_by(Transactions.account_id)
            .order_by(Transactions.date)
            .alias("running_total"),
            "running_total < 10000",
        )
        .build()
    )
    print("Example 2 (multiple where_window conditions — single subquery, AND-ed):")
    print(query2)
    print()

    # Example 3: window function in SELECT only, no filtering
    query3 = (
        Query(Transactions)
        .select(
            Transactions.id,
            Transactions.amount,
            lag(Transactions.amount, 1, "0")
            .partition_by(Transactions.account_id)
            .order_by(Transactions.date)
            .alias("prev_amount"),
            sum_over(Transactions.amount)
            .partition_by(Transactions.account_id)
            .order_by(Transactions.date)
            .alias("running_total"),
        )
        .build()
    )
    print("Example 3 (window functions in SELECT only):")
    print(query3)
    print()

    # Example 4: where_window combined with a CTE
    active_accounts = CTE(
        "active_accounts",
        Query(Accounts)
        .select(Accounts.id, Accounts.balance)
        .where(f"{Accounts.balance} > 0"),
    )
    query4 = (
        Query(Transactions)
        .with_cte(active_accounts)
        .select(Transactions.id, Transactions.account_id, Transactions.amount)
        .join("INNER", "active_accounts", f"{Transactions.account_id} = {Accounts.id}")
        .where_window(
            dense_rank()
            .partition_by(Transactions.account_id)
            .order_by(f"{Transactions.amount} DESC")
            .alias("amount_rank"),
            "amount_rank = 1",
        )
        .build()
    )
    print("Example 4 (where_window + CTE):")
    print(query4)
