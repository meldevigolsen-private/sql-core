from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from db_metadata import AllCols, AllTables  # noqa: F401
    from db_metadata import typed_col as col
    from db_metadata import typed_select as select
else:
    from core_builder import col, select


def build_account_summary_report() -> str:
    """
    Generates a grouped report showing the total amount of transactions,
    transaction count, and max transaction value per account.
    """

    # 1. Aliasing and Aggregating
    query = (
        select(
            # Standard columns
            "t2.account_name",
            "t2.risk_level",
            # Count transactions and alias the column
            col("*").count().as_("total_transactions"),
            # Sum the amount and alias the column
            col("t1.amount").sum().as_("total_volume"),
            # Get the max amount
            col("t1.amount").max().as_("largest_single_transaction"),
        )
        .from_("schema_finance.accounts t2")
        .left_join(
            "schema_finance.transactions t1", col("t2.id") == col("t1.account_id")
        )
        # 2. Filtering
        .where((col("t1.status") == "COMPLETED") & (col("t1.date") >= "2026-01-01"))
        # 3. Grouping and Sorting
        .group_by(col("t2.account_name"), col("t2.risk_level"))
        .order_by(
            # Notice we order by the underlying column/func,
            # sorting by the largest volume desc
            col("t1.amount").sum().desc()
        )
        .limit(50)
    )
    return query.to_sql()


if __name__ == "__main__":
    print("--- Generating Bank Account Summary SQL ---")
    sql = build_account_summary_report()
    print(sql)
