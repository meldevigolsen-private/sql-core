from typing import TYPE_CHECKING

from db_metadata import typed_col as col
from db_metadata import typed_select as select

# --- The Safe Harbor for Metadata Imports ---
if TYPE_CHECKING:
    # Pylance reads these for autocomplete and hover-tooltips.
    # The 'noqa: F401' comment explicitly tells Ruff NOT to delete this unused import.
    from db_metadata import AllCols, AllTables  # noqa: F401


def build_finance_report() -> str:
    # Try typing inside the select() or col() below —
    # Autocomplete will still work perfectly!
    query = (
        select(
            "t1.transaction_id",
            "t1.amount",
            "t2.account_name",
            "t2.account_name",
        )
        .from_("schema_finance.transactions t1")
        .join("schema_finance.accounts t2", col("t1.account_id") == col("t2.id"))
        .where(
            (col("t1.amount") >= 10000)
            & (col("t1.status").in_(["PENDING", "FLAGGED"]))
            & (col("t2.risk_level").not_in(["LOW"]))
        )
        .order_by(col("t1.amount").desc(), col("t1.date").asc())
        .limit(100)
    )
    return query.to_sql()


if __name__ == "__main__":
    print("--- Generating Bank Finance Report SQL ---")
    sql = build_finance_report()
    print(sql)
