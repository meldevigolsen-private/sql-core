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

CTE usage:
  Pass a CTE object directly to Query() to register it and select from it
  in one step:

      my_cte = CTE("active_users", Query(Users).select("*").where("active = true"))
      Query(my_cte).select("*").build()
      # equivalent to: Query("active_users").with_cte(my_cte).select("*").build()
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, Union

if TYPE_CHECKING:
    pass

JOIN_TYPE = Literal["LEFT", "LEFT OUTER", "INNER", "CROSS"]


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

        f"{Transactions}"      # -> "myschema.transactions"
        Transactions.amount    # -> "tx_amount"
    """

    def __str__(cls) -> str:
        name = cls.__name__
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
        .partition_by(*cols)  - PARTITION BY clause (optional)
        .order_by(*cols)      - ORDER BY clause (required for ROW_NUMBER,
                                RANK, DENSE_RANK, LAG, LEAD, FIRST_VALUE,
                                LAST_VALUE; optional for aggregates)
        .alias(name)          - AS alias (required when used in where_window)

    Example:
        row_number().partition_by(Transactions.account_id).order_by(Transactions.date).alias("rn")
        # -> "ROW_NUMBER() OVER (PARTITION BY acc_id ORDER BY tx_date) AS rn"

        lag(Transactions.amount, 1).partition_by(Transactions.account_id).order_by(Transactions.date)
        # -> "LAG(tx_amount, 1) OVER (PARTITION BY acc_id ORDER BY tx_date)"
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


# --- Named window function helpers ---


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


# ---------------------------------------------------------------------------
# CTE
# ---------------------------------------------------------------------------


class CTE:
    """
    Represents a reusable Common Table Expression (WITH clause).

    Defined once from a Query object and can be passed to any number of
    Query instances. Can be used in two ways:

    1. Pass directly to Query() — registers the CTE and selects from it
       in one step (preferred):

           my_cte = CTE("active_users", Query(Users).select("*").where("active = true"))
           Query(my_cte).select("*").build()

    2. Register manually via .with_cte() — useful when the CTE is not the
       primary table but is referenced in joins or conditions:

           Query(Orders).with_cte(my_cte).select("*").join("INNER", my_cte, ...).build()

    For filtering on window function results, prefer .where_window() on
    the Query directly — it handles the subquery wrapping automatically.
    Only use CTE manually when you need to reference the windowed result
    in multiple subsequent queries.
    """

    def __init__(self, name: str, query: Query) -> None:
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
# Helpers
# ---------------------------------------------------------------------------

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
        table: A BaseTable subclass, CTE object, or plain string table name.
        condition: The ON condition.

    Note: CROSS JOIN does not use an ON condition in Netezza, but passing
    one here will not cause a syntax error - Netezza will raise at query time.
    """
    resolved = _resolve_table(table)
    if not resolved:
        raise ValueError("Table name cannot be empty")
    if not condition:
        raise ValueError("Join condition cannot be empty")
    return f"{join_type} JOIN {resolved} ON {condition}"


# ---------------------------------------------------------------------------
# Query builder
# ---------------------------------------------------------------------------


class Query:
    """
    Fluent query builder for Netezza SQL.

    Accepts BaseTable subclasses, CTE objects, or plain strings as table
    references throughout (Query(), .join()). Column attributes from
    BaseTable subclasses are plain strings and work naturally with all
    builder methods. WindowFunction expressions can be passed to .select().

    Passing a CTE to Query() registers it automatically:

        active_users = CTE("active_users", Query(Users).select("*").where("active = true"))

        # These are equivalent:
        Query(active_users).select("*").build()
        Query("active_users").with_cte(active_users).select("*").build()

    To filter on a window function result use .where_window() - it
    automatically wraps the query in a subquery so the condition can be
    applied legally. Multiple .where_window() calls are combined into a
    single subquery level and AND-ed together.
    """

    def __init__(self, table: TableRef) -> None:
        """
        Args:
            table: A BaseTable subclass, a CTE object, or a plain string.
                   - BaseTable subclasses resolve to their schema-qualified name.
                   - CTE objects register themselves automatically and use their
                     name as the table reference, so Query(my_cte) is equivalent
                     to Query("my_cte").with_cte(my_cte).
                   - Plain strings are used as-is (e.g. for literal table names).
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

    def with_cte(self, *ctes: CTE) -> Query:
        """
        Add one or more CTEs to the query.

        CTEs are rendered in the order they are added. If a CTE references
        another CTE, the referenced CTE must be added first.

        Note: if you are selecting directly from a CTE, prefer passing it
        to Query() instead of using .with_cte() — it's more concise.
        Use .with_cte() when the CTE is referenced in joins or conditions
        rather than as the primary FROM table.
        """
        names = [c.name for c in self._ctes]
        for cte in ctes:
            if cte.name in names:
                raise ValueError(f"Duplicate CTE name: '{cte.name}'")
            self._ctes.append(cte)
            names.append(cte.name)
        return self

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

    def where(self, condition: str) -> Query:
        if not condition:
            raise ValueError("WHERE condition cannot be empty")
        self._where = condition
        return self

    def where_window(self, window_func: WindowFunction, condition: str) -> Query:
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

    def join(self, join_type: JOIN_TYPE, table: TableRef, condition: str) -> Query:
        """
        Args:
            join_type: One of the supported Netezza join types.
            table: A BaseTable subclass, CTE object, or plain string.
            condition: The ON condition.
        """
        self._joins.append(join(join_type, table, condition))
        return self

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

    def _build_inner(self) -> str:
        """Build the inner SELECT used when where_window wraps the query in a subquery.
        Includes all selected columns plus any window function columns needed for filtering."""
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

    # Example 1: Query(cte) — CTE registered and selected from in one step
    active_accounts = CTE(
        "active_accounts",
        Query(Accounts)
        .select(Accounts.id, Accounts.balance)
        .where(f"{Accounts.balance} > 0"),
    )
    query1 = (
        Query(active_accounts)  # no .with_cte() needed
        .select(Accounts.id, Accounts.balance)
        .where(f"{Accounts.balance} > 500")
        .order_by(Accounts.balance)
        .build()
    )
    print("Example 1 (Query(cte) — automatic registration):")
    print(query1)
    print()

    # Example 2: Query(cte) + where_window
    windowed_txns = CTE(
        "windowed_txns",
        Query(Transactions)
        .select(Transactions.id, Transactions.account_id, Transactions.amount)
        .where(f"{Transactions.status} = 'active'"),
    )
    query2 = (
        Query(windowed_txns)
        .select(Transactions.id, Transactions.account_id, Transactions.amount)
        .where_window(
            dense_rank()
            .partition_by(Transactions.account_id)
            .order_by(f"{Transactions.amount} DESC")
            .alias("amount_rank"),
            "amount_rank = 1",
        )
        .build()
    )
    print("Example 2 (Query(cte) + where_window):")
    print(query2)
    print()

    # Example 3: .with_cte() still works for CTEs used in joins rather than FROM
    accounts_cte = CTE(
        "accounts_cte",
        Query(Accounts)
        .select(Accounts.id, Accounts.balance)
        .where(f"{Accounts.balance} > 0"),
    )
    query3 = (
        Query(Transactions)
        .with_cte(accounts_cte)  # CTE joined, not the primary table
        .select(Transactions.id, Transactions.amount, Accounts.balance)
        .join("INNER", accounts_cte, f"{Transactions.account_id} = {Accounts.id}")
        .where(f"{Transactions.status} = 'active'")
        .build()
    )
    print("Example 3 (.with_cte() for a joined CTE):")
    print(query3)
    print()

    # Example 4: window functions in SELECT only, no filtering
    query4 = (
        Query(Transactions)
        .select(
            Transactions.id,
            Transactions.amount,
            row_number()
            .partition_by(Transactions.account_id)
            .order_by(f"{Transactions.date} DESC")
            .alias("rn"),
            sum_over(Transactions.amount)
            .partition_by(Transactions.account_id)
            .order_by(Transactions.date)
            .alias("running_total"),
            lag(Transactions.amount, 1, "0")
            .partition_by(Transactions.account_id)
            .order_by(Transactions.date)
            .alias("prev_amount"),
        )
        .build()
    )
    print("Example 4 (window functions in SELECT only):")
    print(query4)
