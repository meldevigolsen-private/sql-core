import sqlite3
from typing import Dict, List

import pyodbc


def get_sqlite_schema(db_path: str) -> Dict[str, List[str]]:
    """Extracts tables and columns from a SQLite database."""
    print(f"Connecting to SQLite database at {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Mocking some tables for demonstration if the db is empty or purely in-memory
    if db_path == ":memory:":
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS transactions (id INT, account_id INT, amount REAL, status TEXT, date TEXT)"
        )
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS accounts (id INT, account_name TEXT, account_type TEXT)"
        )

    cursor.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view');")
    tables = [row[0] for row in cursor.fetchall()]

    schema_dict: Dict[str, List[str]] = {}
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table});")
        columns = [row[1] for row in cursor.fetchall()]
        schema_dict[table] = columns

    conn.close()
    return schema_dict


def get_odbc_schema(conn_str: str, dialect: str) -> Dict[str, List[str]]:
    """
    Extracts tables, views, and columns from enterprise DBs via ODBC.
    Supported dialects: 'mssql', 'azure', 'netezza'
    """
    if pyodbc is None:
        raise ImportError(
            "pyodbc is required for MSSQL, Azure, and Netezza. Run `pip install pyodbc`."
        )

    print(f"Connecting to {dialect.upper()} database via ODBC...")
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    schema_dict: Dict[str, List[str]] = {}

    if dialect in ("mssql", "azure"):
        # INFORMATION_SCHEMA.COLUMNS includes both Tables and Views in SQL Server / Azure
        query = """
            SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
        """
        cursor.execute(query)
        for row in cursor.fetchall():
            schema, table, col = row[0], row[1], row[2]
            full_table = f"{schema}.{table}" if schema else table
            schema_dict.setdefault(full_table, []).append(col)

    elif dialect == "netezza":
        # _V_RELATION_COLUMN is the Netezza system view containing both table and view columns
        query = """
            SELECT SCHEMA AS TABLE_SCHEMA, TABLENAME AS TABLE_NAME, ATTNAME AS COLUMN_NAME
            FROM _V_RELATION_COLUMN
            ORDER BY SCHEMA, TABLENAME, ATTNUM
        """
        cursor.execute(query)
        for row in cursor.fetchall():
            schema, table, col = row[0], row[1], row[2]
            full_table = f"{schema}.{table}" if schema else table
            schema_dict.setdefault(full_table, []).append(col)

    conn.close()
    return schema_dict


def generate_stubs(db_type: str, connection_info: str, output_path: str):
    """
    Main orchestrator to fetch schema and write it to a .pyi stub file.
    db_type: 'sqlite', 'mssql', 'azure', or 'netezza'
    connection_info: File path for sqlite, or ODBC connection string for others.
    """
    if db_type == "sqlite":
        schema_dict = get_sqlite_schema(connection_info)
    elif db_type in ("mssql", "azure", "netezza"):
        schema_dict = get_odbc_schema(connection_info, db_type)
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

    # Build the .pyi file content
    lines = [
        "from typing import Literal, Union, Optional",
        "from core_builder import Query, col",
        "\n# --- 1. Auto-Generated Tables & Views ---",
    ]

    table_literals = []
    # Make table names safe for Python variable names (replace dots and hyphens)
    for table in schema_dict.keys():
        safe_name = table.replace(".", "_").replace("-", "_").capitalize()
        literal_name = f"{safe_name}Table"
        lines.append(f'{literal_name} = Literal["{table}"]')
        table_literals.append(literal_name)

    lines.append(f"AllTables = Union[{', '.join(table_literals)}]\n")

    lines.append("# --- 2. Auto-Generated Columns ---")
    column_literals = []
    for table, cols in schema_dict.items():
        safe_name = table.replace(".", "_").replace("-", "_").capitalize()
        literal_name = f"{safe_name}Cols"
        # Prefix columns with table name for uniqueness like "transactions.amount"
        # We also allow standard columns and the wildcard '*'
        formatted_cols = (
            [f'"{table}.{c}"' for c in cols] + [f'"{c}"' for c in cols] + ['"*"']
        )
        lines.append(f"{literal_name} = Literal[{', '.join(formatted_cols)}]")
        column_literals.append(literal_name)

    lines.append(f"AllCols = Union[{', '.join(column_literals)}]\n")

    lines.append("# --- 3. Strongly Typed Wrappers ---")
    lines.append(
        "def typed_select(*columns: Union[AllCols, 'typed_col']) -> Query: ...\n"
    )

    lines.append("class typed_col(col):")
    lines.append(
        "    def __init__(self, name: AllCols, alias: Optional[str] = None, func: Optional[str] = None): ..."
    )
    lines.append("    def as_(self, alias: str) -> 'typed_col': ...")
    lines.append("    def sum(self) -> 'typed_col': ...")
    lines.append("    def count(self) -> 'typed_col': ...")
    lines.append("    def max(self) -> 'typed_col': ...")
    lines.append("    def min(self) -> 'typed_col': ...")
    lines.append("    def avg(self) -> 'typed_col': ...")

    # Write to disk
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(
        f"Successfully generated stubs at {output_path} with {len(schema_dict)} tables/views extracted."
    )


if __name__ == "__main__":
    # --- Example 1: SQLite ---
    generate_stubs(
        db_type="sqlite", connection_info=":memory:", output_path="db_metadata.pyi"
    )

    # --- Example 2: Microsoft SQL Server / Azure SQL ---
    # mssql_conn_str = "Driver={ODBC Driver 17 for SQL Server};Server=my_server.database.windows.net;Database=my_db;Uid=my_user;Pwd=my_password;"
    # generate_stubs("azure", mssql_conn_str, "azure_metadata.pyi")

    # --- Example 3: Netezza ---
    # netezza_conn_str = "Driver={NetezzaSQL};Server=nz_host;Port=5480;Database=nz_db;Uid=admin;Pwd=password;"
    # generate_stubs("netezza", netezza_conn_str, "netezza_metadata.pyi")
