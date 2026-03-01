"""
Table metadata layer for the Netezza query builder.

Defines BaseTable and its metaclass TableMeta. Column attributes can be either
plain strings (legacy) or Column() instances (preferred, supports descriptions).
In both cases, accessing the attribute in a Query expression returns a fully-
qualified SQL reference: "schema.TableName.column_name".

Because Column is a str subclass, it satisfies all str annotations throughout
the query builder — no casts or overloads required. Type checkers see Column
attributes as str, so they work in .select(), .order_by(), .partition_by(),
window function helpers, and anywhere else a str column reference is expected.

Example:
    class Transactions(BaseTable):
        __schema__      = "myschema"
        __description__ = "All financial transactions."

        id         = Column("tx_id",     description="Primary key.")
        account_id = Column("acc_id",    description="FK to Accounts.")
        amount     = Column("tx_amount", description="Transaction amount in USD.")

    f"{Transactions}"                    # -> "myschema.Transactions"
    Transactions.amount                  # -> "myschema.Transactions.tx_amount"
    Transactions.columns()               # -> {"id": Column(...), ...}
    Transactions.raw_column("amount")    # -> "tx_amount"
"""

from __future__ import annotations

from .column import Column

# Attributes that should never be treated as column definitions.
_SKIP = frozenset(
    {
        "__schema__",
        "__description__",
        "__doc__",
        "__module__",
        "__qualname__",
        "__dict__",
        "__weakref__",
    }
)


class TableMeta(type):
    """
    Metaclass for BaseTable.

    - str(SomeTable)           -> "schema.SomeTable" or "SomeTable"
    - SomeTable.col            -> "schema.SomeTable.db_col_name"  (fully-qualified)
    - SomeTable.columns()      -> {attr: Column, ...}
    - SomeTable.raw_column()   -> bare DB column name
    """

    def __str__(cls) -> str:
        schema = getattr(cls, "__schema__", None)
        return f"{schema}.{cls.__name__}" if schema else cls.__name__

    @property
    def table_name(cls) -> str:
        return str(cls)

    def __getattribute__(cls, name: str) -> object:
        value = super().__getattribute__(name)

        # Pass through dunder/private names and explicitly skipped attributes.
        if name.startswith("_") or name in _SKIP:
            return value

        # Column is a str subclass, so this single isinstance check handles
        # both Column() definitions and legacy plain-string definitions.
        # Returns the fully-qualified SQL reference: "schema.Table.col_name".
        if isinstance(value, str):
            return f"{cls}.{value}"

        return value


class BaseTable(metaclass=TableMeta):
    """
    Base class for table definitions.

    Subclass to declare tables. Use Column() for column attributes to attach
    descriptions; plain strings are still accepted for compatibility.

    Because Column is a str subclass, all column attributes satisfy str
    annotations and work directly in every query builder method.

    Class-level fields:
        __schema__:      Optional schema name — table resolves to "schema.ClassName".
        __description__: Human-readable description of the table (optional).

    Introspection (called on the class, not instances):
        columns()           {attr: Column} for every declared column.
                            Plain strings are wrapped in Column with no description.
        raw_column(attr)    The bare DB column name for a given attribute name.

    Example:
        class Accounts(BaseTable):
            __schema__      = "myschema"
            __description__ = "Customer bank accounts."

            id      = Column("acc_id",          description="Primary key.")
            balance = Column("current_balance", description="Current balance.")

        Accounts.balance                  # "myschema.Accounts.current_balance"
        Accounts.raw_column("balance")    # "current_balance"
    """

    __schema__: str | None = None
    __description__: str = ""

    @classmethod
    def columns(cls) -> dict[str, Column]:
        """
        Return all declared column definitions keyed by Python attribute name.

        Plain string definitions are wrapped in Column (no description) so
        callers always receive a uniform type. Values carry the raw DB column
        name, not the qualified one.
        """
        result: dict[str, Column] = {}
        for attr, value in vars(cls).items():
            if attr.startswith("_") or attr in _SKIP:
                continue
            if isinstance(value, Column):
                result[attr] = value
            elif isinstance(value, str):
                result[attr] = Column(value)
        return result

    @classmethod
    def raw_column(cls, attr: str) -> str:
        """
        Return the bare DB column name for a given Python attribute name.

        Useful when you need the unqualified name, e.g. for Relationship
        declarations or raw SQL fragments.

        Raises AttributeError if the attribute is not a declared column.
        """
        value = vars(cls).get(attr)
        if isinstance(value, str):  # covers Column (str subclass) and plain str
            return str(value)  # str() strips any Column subclass wrapping
        raise AttributeError(f"{cls.__name__!r} has no column attribute {attr!r}")
