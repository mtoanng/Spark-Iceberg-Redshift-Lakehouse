"""Small Redshift Data API runner for reconciliation and verification."""

from __future__ import annotations

from dataclasses import dataclass
import time
from typing import Any, Callable, Mapping


class RedshiftQueryError(RuntimeError):
    """Raised when a Redshift Data API statement does not finish successfully."""


@dataclass(frozen=True)
class RedshiftQueryResult:
    statement_id: str
    rows: tuple[tuple[object | None, ...], ...]


def _cell_value(cell: Mapping[str, object]) -> object | None:
    if cell.get("isNull") is True:
        return None
    for key in ("longValue", "doubleValue", "booleanValue", "stringValue", "blobValue"):
        if key in cell:
            return cell[key]
    raise RedshiftQueryError("Redshift Data API returned an unsupported result cell.")


def run_query(
    client: Any,
    *,
    database: str,
    workgroup_name: str,
    sql: str,
    statement_name: str,
    parameters: Mapping[str, object] | None = None,
    timeout_seconds: float = 60.0,
    poll_interval_seconds: float = 1.0,
    sleep: Callable[[float], None] = time.sleep,
) -> RedshiftQueryResult:
    request: dict[str, object] = {
        "WorkgroupName": workgroup_name,
        "Database": database,
        "Sql": sql,
        "StatementName": statement_name,
    }
    if parameters:
        request["Parameters"] = [
            {"name": name, "value": str(value)} for name, value in parameters.items()
        ]
    statement_id = client.execute_statement(**request)["Id"]
    deadline = time.monotonic() + timeout_seconds
    while True:
        details = client.describe_statement(Id=statement_id)
        status = details["Status"]
        if status == "FINISHED":
            records = client.get_statement_result(Id=statement_id).get("Records", [])
            return RedshiftQueryResult(
                statement_id=statement_id,
                rows=tuple(
                    tuple(_cell_value(cell) for cell in record) for record in records
                ),
            )
        if status in {"FAILED", "ABORTED"}:
            raise RedshiftQueryError(
                f"Redshift statement {statement_id} {status}: "
                f"{details.get('Error', 'no error supplied')}"
            )
        if time.monotonic() >= deadline:
            raise RedshiftQueryError(
                f"Redshift statement {statement_id} timed out after "
                f"{timeout_seconds} seconds."
            )
        sleep(poll_interval_seconds)
