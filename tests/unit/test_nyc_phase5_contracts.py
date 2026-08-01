"""Local contracts for bounded manual backfill requests."""

from __future__ import annotations

import pytest

from etl.orchestration.nyc_hvfhs_runs import (
    sequential_backfill_requests,
)
from etl.sources.nyc_hvfhs import SourceContractError


def test_four_month_backfill_is_sequential_and_bounded() -> None:
    requests = sequential_backfill_requests(2024, 1)

    assert [(request.year, request.month) for request in requests] == [
        (2024, 1),
        (2024, 2),
        (2024, 3),
        (2024, 4),
    ]
    with pytest.raises(SourceContractError, match="September"):
        sequential_backfill_requests(2024, 10)
