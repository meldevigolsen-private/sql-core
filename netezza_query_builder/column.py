"""
Column descriptor for BaseTable definitions.

Instead of bare strings, use Column() to attach a description alongside the
database column name:

    class Accounts(BaseTable):
        __schema__ = "myschema"
        __description__ = "Customer bank accounts."

        id      = Column("acc_id",          description="Primary key.")
        balance = Column("current_balance", description="Current account balance in USD.")

Column subclasses str, so it is accepted everywhere a str is expected —
including Query.select(), Query.order_by(), WindowFunction.partition_by(),
and all other builder methods. The str value is the physical DB column name.

TableMeta.__getattribute__ intercepts attribute access and returns a fully-
qualified SQL string ("schema.Table.col_name") at runtime. Type checkers see
Column as str and raise no errors.

The extra metadata (description) is accessible via BaseTable.columns() for
documentation or visualisation.
"""

from __future__ import annotations


class Column(str):
    """
    Describes a single database column.

    Subclasses str so it satisfies str type annotations everywhere in the
    query builder. The string value is the physical column name as it exists
    in the database.

    Args:
        name:        The physical column name as it exists in the database.
        description: Human-readable description of the column (optional).

    Example:
        amount = Column("tx_amount", description="Transaction amount in USD.")
        # str(amount) == "tx_amount"
        # amount.description == "Transaction amount in USD."
    """

    # __new__ is used because str is immutable — value must be set at creation.
    def __new__(cls, name: str, description: str = "") -> Column:
        if not name:
            raise ValueError("Column name cannot be empty")
        instance = super().__new__(cls, name)
        return instance

    def __init__(self, name: str, description: str = "") -> None:
        # str.__init__ takes no args beyond self; description is stored here.
        super().__init__()
        self.description = description

    @property
    def name(self) -> str:
        """The physical DB column name (same as the str value)."""
        return str(self)

    def __repr__(self) -> str:
        desc = f", description={self.description!r}" if self.description else ""
        return f"Column({str(self)!r}{desc})"
