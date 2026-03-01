from typing import Literal, Union

from core_builder import Query, col

# --- 1. Define Valid Tables ---
FinanceTables = Literal[
    "schema_finance.transactions", "schema_finance.accounts", "schema_finance.customers"
]
HRTables = Literal["schema_hr.employees", "schema_hr.payroll"]
AllTables = Union[FinanceTables, HRTables]

# --- 2. Define Valid Columns ---
TransactionsCols = Literal[
    "t1.transaction_id", "t1.account_id", "t1.amount", "t1.status", "t1.date"
]
AccountsCols = Literal[
    "t2.id", "t2.account_name", "t2.account_type", "t2.risk_level", "t2.customer_id"
]
AllCols = Union[TransactionsCols, AccountsCols]

# --- 3. Strongly Typed Wrappers for the IDE ---
# We override the signatures of `select` and `col` here in the stub file
# so the IDE knows exactly what string literals are allowed.

def typed_select(*columns: AllCols) -> Query: ...

class typed_col(col):
    def __init__(self, name: AllCols): ...
