"""Deterministic request and audit contracts for NYC HVFHV manual runs."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from etl.sources.nyc_hvfhs import SourceContractError, SourceFile, stable_run_id


@dataclass(frozen=True)
class MonthlyRunRequest:
    """One immutable-source orchestration request."""

    year: int
    month: int

    def __post_init__(self) -> None:
        if not 2019 <= self.year <= 2099:
            raise SourceContractError("year must be between 2019 and 2099.")
        if not 1 <= self.month <= 12:
            raise SourceContractError("month must be between 1 and 12.")


@dataclass(frozen=True)
class RunAudit:
    """Serializable audit facts for a DAG run; no credentials are stored."""

    run_id: str
    source_year: int
    source_month: int
    source_uri: str
    source_checksum: str
    source_size_bytes: int
    requested_at: datetime


def audit_for_source(
    request: MonthlyRunRequest,
    source: SourceFile,
    *,
    requested_at: datetime | None = None,
) -> RunAudit:
    """Bind an orchestrator request to one immutable source identity."""

    if (source.source_year, source.source_month) != (request.year, request.month):
        raise SourceContractError("Run request and source year/month must match.")
    return RunAudit(
        run_id=stable_run_id(source),
        source_year=source.source_year,
        source_month=source.source_month,
        source_uri=source.source_uri,
        source_checksum=source.source_checksum,
        source_size_bytes=source.source_size_bytes,
        requested_at=requested_at or datetime.now(timezone.utc),
    )


def sequential_backfill_requests(
    year: int, first_month: int
) -> tuple[MonthlyRunRequest, ...]:
    """Return exactly four ordered manual runs without crossing a year boundary."""

    first = MonthlyRunRequest(year=year, month=first_month)
    if first.month > 9:
        raise SourceContractError(
            "A four-month backfill must start no later than September."
        )
    return tuple(
        MonthlyRunRequest(year=first.year, month=first.month + offset)
        for offset in range(4)
    )
