"""
Top-level facade and example usage for the SQL builder package.
This file re-exports the main classes/functions and keeps the original
`__main__` examples for convenience.
"""

from cte import CTE
from query import Query
from tables import BaseTable
from window import (
    dense_rank,
    lag,
    row_number,
    sum_over,
)

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

    active_accounts = CTE(
        "active_accounts",
        Query(Accounts)
        .select(Accounts.id, Accounts.balance)
        .where(f"{Accounts.balance} > 0"),
    )
    query1 = (
        Query(active_accounts)
        .select(Accounts.id, Accounts.balance)
        .where(f"{Accounts.balance} > 500")
        .order_by(Accounts.balance)
        .build()
    )
    print("Example 1 (Query(cte) — automatic registration):")
    print(query1)
    print()

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

    accounts_cte = CTE(
        "accounts_cte",
        Query(Accounts)
        .select(Accounts.id, Accounts.balance)
        .where(f"{Accounts.balance} > 0"),
    )
    query3 = (
        Query(Transactions)
        .with_cte(accounts_cte)
        .select(Transactions.id, Transactions.amount, Accounts.balance)
        .join("INNER", accounts_cte, f"{Transactions.account_id} = {Accounts.id}")
        .where(f"{Transactions.status} = 'active'")
        .build()
    )
    print("Example 3 (.with_cte() for a joined CTE):")
    print(query3)
    print()

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
