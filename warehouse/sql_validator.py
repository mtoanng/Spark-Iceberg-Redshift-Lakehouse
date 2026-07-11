"""
SQL validator using sqlglot (AST-based) — rejects all non-SELECT statements.

The warehouse service only accepts read-only queries. Instead of naive string
matching (e.g. ``sql.startswith("SELECT")``), we parse the SQL into an AST with
``sqlglot`` and inspect the node types. This catches multi-statement injection,
nested DDL inside CTEs, and other tricks that defeat regex.
"""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from typing import Tuple

# Node types that represent read-only, safe expressions.
_READONLY_ROOTS = (exp.Select, exp.Subquery, exp.Union, exp.Intersect, exp.Except)

# Explicitly forbidden node types — even if they appear inside a CTE.
# Note: attribute names must match the installed sqlglot version.
_FORBIDDEN_NODES = tuple(
    node
    for node in (
        getattr(exp, name, None)
        for name in (
            "Insert", "Update", "Delete", "Drop", "Create",
            "Alter", "AlterTable", "TruncateTable",
            "Command",   # covers COPY, SET, PRAGMA, etc. (non-AST commands)
            "Merge", "Vacuum",
        )
    )
    if node is not None
)


class SQLValidationError(Exception):
    """Raised when a SQL statement is not a permitted read-only query."""


def validate_sql(sql: str) -> Tuple[bool, str]:
    """
    Validate that *sql* is a read-only SELECT or WITH (CTE) statement.

    Uses ``sqlglot`` to parse the SQL into an AST and then inspects every
    node to ensure no DDL/DML is present.

    Args:
        sql: Raw SQL string from the user.

    Returns:
        ``(True, "")`` when the query is safe to execute.
        ``(False, reason)`` when the query must be rejected.
    """
    if not sql or not sql.strip():
        return False, "Empty SQL statement."

    # ------------------------------------------------------------------
    # 1. Parse — if sqlglot cannot parse, reject rather than guess.
    # ------------------------------------------------------------------
    try:
        parsed = sqlglot.parse(sql, read="duckdb")
    except Exception as exc:  # sqlglot raises various error subclasses
        return False, f"SQL parse error: {exc}"

    # Reject multi-statement input — only one statement allowed.
    if len(parsed) != 1:
        return False, (
            f"Multiple statements detected ({len(parsed)} found). "
            "Only a single SELECT/WITH query is permitted."
        )

    tree = parsed[0]
    if tree is None:
        return False, "Empty SQL statement after parsing."

    # ------------------------------------------------------------------
    # 2. Walk the entire AST and reject any forbidden node, even if nested.
    # ------------------------------------------------------------------
    for node in tree.walk():
        if isinstance(node, _FORBIDDEN_NODES):
            return False, (
                f"Statement type '{type(node).__name__}' is not permitted. "
                "Only read-only SELECT queries are allowed."
            )

    # ------------------------------------------------------------------
    # 3. Verify the root node is a read-only expression.
    # ------------------------------------------------------------------
    # A CTE (WITH ... SELECT ...) parses to a Select with a `with` attribute,
    # so checking for exp.Select covers both plain SELECT and WITH/CTE.
    if not isinstance(tree, _READONLY_ROOTS):
        return False, (
            f"Root statement type '{type(tree).__name__}' is not a SELECT. "
            "Only read-only SELECT queries are allowed."
        )

    return True, ""
