from typing import Any, List, Optional, Tuple, Union


class Expression:
    def __init__(
        self, left: Any, operator: str, right: Any = None, is_group: bool = False
    ):
        self.left = left
        self.operator = operator
        self.right = right
        self.is_group = is_group

    def __and__(self, other: "Expression") -> "Expression":
        return Expression(self, "AND", other, is_group=True)

    def __or__(self, other: "Expression") -> "Expression":
        return Expression(self, "OR", other, is_group=True)

    def compile(self) -> str:
        if self.is_group:
            return f"({self.left.compile()} {self.operator} {self.right.compile()})"

        if self.operator.upper() in ("IS NULL", "IS NOT NULL"):
            return f"{self.left} {self.operator}"

        val = self.right

        if isinstance(val, col):
            formatted_val = val.name
        elif self.operator.upper() in ("IN", "NOT IN"):
            if not isinstance(val, (list, tuple)):
                raise ValueError(f"{self.operator} requires a list or tuple.")
            items = []
            for v in val:
                if isinstance(v, str):
                    items.append(f"'{v}'")
                elif v is None:
                    items.append("NULL")
                else:
                    items.append(str(v))
            formatted_val = f"({', '.join(items)})"
        elif isinstance(val, str):
            formatted_val = f"'{val}'"
        elif val is None:
            formatted_val = "NULL"
        else:
            formatted_val = str(val)

        return f"{self.left} {self.operator} {formatted_val}"


class SortExpression:
    def __init__(self, col_name: str, direction: str):
        self.col_name = col_name
        self.direction = direction

    def compile(self) -> str:
        return f"{self.col_name} {self.direction}"


class col:
    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other) -> Expression:  # type: ignore[override]
        return Expression(self.name, "=", other)

    def __ne__(self, other) -> Expression:  # type: ignore[override]
        return Expression(self.name, "<>", other)

    def __gt__(self, other) -> Expression:
        return Expression(self.name, ">", other)

    def __lt__(self, other) -> Expression:
        return Expression(self.name, "<", other)

    def __ge__(self, other) -> Expression:
        return Expression(self.name, ">=", other)

    def __le__(self, other) -> Expression:
        return Expression(self.name, "<=", other)

    def is_null(self) -> Expression:
        return Expression(self.name, "IS NULL")

    def is_not_null(self) -> Expression:
        return Expression(self.name, "IS NOT NULL")

    def in_(self, values: Union[list, tuple]) -> Expression:
        return Expression(self.name, "IN", values)

    def not_in(self, values: Union[list, tuple]) -> Expression:
        return Expression(self.name, "NOT IN", values)

    def asc(self) -> SortExpression:
        return SortExpression(self.name, "ASC")

    def desc(self) -> SortExpression:
        return SortExpression(self.name, "DESC")

    def __str__(self):
        return self.name


class Query:
    def __init__(self, columns: tuple):
        self._select: List[str] = [
            c.name if isinstance(c, col) else str(c) for c in columns
        ]
        self._from: Optional[str] = None
        self._where_expr: Optional[Expression] = None
        self._joins: List[Tuple[str, str, Expression]] = []
        self._order_by: List[str] = []
        self._limit: Optional[int] = None

    def from_(self, table_name: str) -> "Query":
        self._from = table_name
        return self

    def join(
        self, table_name: str, condition: Expression, join_type: str = "INNER"
    ) -> "Query":
        self._joins.append((join_type.upper(), table_name, condition))
        return self

    def left_join(self, table_name: str, condition: Expression) -> "Query":
        return self.join(table_name, condition, "LEFT")

    def where(self, expression: Expression) -> "Query":
        self._where_expr = expression
        return self

    def order_by(self, *expressions: Union[str, col, SortExpression]) -> "Query":
        for expr in expressions:
            if isinstance(expr, SortExpression):
                self._order_by.append(expr.compile())
            elif isinstance(expr, col):
                self._order_by.append(f"{expr.name} ASC")
            else:
                self._order_by.append(f"{str(expr)} ASC")
        return self

    def limit(self, n: int) -> "Query":
        self._limit = n
        return self

    def to_sql(self) -> str:
        if not self._from:
            raise ValueError("Incomplete SQL: FROM clause is missing.")

        cols = ", ".join(self._select) if self._select else "*"
        sql_parts = [f"SELECT {cols} FROM {self._from}"]

        for j_type, j_table, j_cond in self._joins:
            sql_parts.append(f"{j_type} JOIN {j_table} ON {j_cond.compile()}")

        if self._where_expr:
            sql_parts.append(f"WHERE {self._where_expr.compile()}")

        if self._order_by:
            sql_parts.append(f"ORDER BY {', '.join(self._order_by)}")

        if self._limit is not None:
            sql_parts.append(f"LIMIT {self._limit}")

        return " ".join(sql_parts)


def select(*columns: Union[str, col]) -> Query:
    return Query(columns)
