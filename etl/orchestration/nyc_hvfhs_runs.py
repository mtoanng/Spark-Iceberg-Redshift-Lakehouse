"""Validated month requests for NYC HVFHV manual runs and backfills."""

from __future__ import annotations

from dataclasses import dataclass

from etl.sources.nyc_hvfhs import SourceContractError


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
