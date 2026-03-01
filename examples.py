"""
Example usage of the Netezza query builder.
Run directly: python examples.py
"""

import json

from netezza_query_builder import (
    CTE,
    BaseTable,
    Column,
    Query,
    dense_rank,
    get_relationships,
    graph_data,
    lag,
    relate,
    row_number,
    sum_over,
)

# ---------------------------------------------------------------------------
# Table definitions — now with Column() and __description__
# ---------------------------------------------------------------------------


class Accounts(BaseTable):
    __schema__ = "myschema"
    __description__ = "Customer bank accounts."

    id = Column("acc_id", description="Primary key.")
    owner = Column("owner_name", description="Full name of the account holder.")
    balance = Column("current_balance", description="Current balance in USD.")


class Transactions(BaseTable):
    __schema__ = "myschema"
    __description__ = "All financial transactions."

    id = Column("tx_id", description="Primary key.")
    account_id = Column(
        "acc_id", description="FK — the account this transaction belongs to."
    )
    amount = Column("tx_amount", description="Transaction amount in USD.")
    status = Column(
        "tx_status", description="Processing status: active | pending | failed."
    )
    date = Column("tx_date", description="UTC timestamp of the transaction.")


class AuditLog(BaseTable):
    __schema__ = "myschema"
    __description__ = "Immutable audit trail for all account mutations."

    id = Column("audit_id", description="Primary key.")
    account_id = Column("acc_id", description="FK — the affected account.")
    changed_by = Column("changed_by", description="Username that triggered the change.")
    changed_at = Column("changed_at", description="UTC timestamp of the change.")


# ---------------------------------------------------------------------------
# Relationship declarations
# ---------------------------------------------------------------------------

relate(
    Transactions,
    "account_id",
    Accounts,
    "id",
    cardinality="many-to-one",
    description="Each transaction belongs to exactly one account.",
)

relate(
    AuditLog,
    "account_id",
    Accounts,
    "id",
    cardinality="many-to-one",
    description="Each audit entry records a change on one account.",
)


# ---------------------------------------------------------------------------
# SQL query examples (unchanged from before, now using Column attrs)
# ---------------------------------------------------------------------------

print("=" * 60)
print("SQL EXAMPLES")
print("=" * 60)

# Example 1: CTE + filter
active_accounts = CTE(
    "active_accounts",
    Query(Accounts)
    .select(Accounts.id, Accounts.balance)
    .where(f"{Accounts.balance} > 0"),
)
print("\nExample 1 — CTE + filter:")
print(
    Query(active_accounts)
    .select(Accounts.id, Accounts.balance)
    .where(f"{Accounts.balance} > 500")
    .order_by(Accounts.balance)
    .build()
)

# Example 2: CTE + where_window
windowed_txns = CTE(
    "windowed_txns",
    Query(Transactions)
    .select(Transactions.id, Transactions.account_id, Transactions.amount)
    .where(f"{Transactions.status} = 'active'"),
)
print("\nExample 2 — CTE + where_window:")
print(
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

# Example 3: joined CTE
accounts_cte = CTE(
    "accounts_cte",
    Query(Accounts)
    .select(Accounts.id, Accounts.balance)
    .where(f"{Accounts.balance} > 0"),
)
print("\nExample 3 — joined CTE:")
print(
    Query(Transactions)
    .with_cte(accounts_cte)
    .select(Transactions.id, Transactions.amount, Accounts.balance)
    .join("INNER", accounts_cte, f"{Transactions.account_id} = {Accounts.id}")
    .where(f"{Transactions.status} = 'active'")
    .build()
)

# Example 4: window functions in SELECT
print("\nExample 4 — window functions in SELECT:")
print(
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


# ---------------------------------------------------------------------------
# Introspection examples
# ---------------------------------------------------------------------------

print("\n" + "=" * 60)
print("INTROSPECTION")
print("=" * 60)

print(f"\nTransactions table  : {Transactions}")
print(f"  description       : {Transactions.__description__}")
print("  columns:")
for attr, col in Transactions.columns().items():
    desc = f"  — {col.description}" if col.description else ""
    print(f"    {attr:12s}  ->  {col.name}{desc}")

print(f"\nraw_column('amount'): {Transactions.raw_column('amount')}")

print("\nRelationships touching Accounts:")
for rel in get_relationships(Accounts):
    print(f"  {rel}  [{rel.cardinality}]")
    print(f"    join: {rel.join_condition()}")
    if rel.description:
        print(f"    note: {rel.description}")

print("\nFull graph_data() (JSON):")
print(json.dumps(graph_data(), indent=2))
