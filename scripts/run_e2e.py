"""Bounded, unexecuted E2E command plan for the four-month release profile."""

from __future__ import annotations

import argparse
import json

from etl.orchestration.nyc_hvfhs_runs import (
    MonthlyRunRequest,
    sequential_backfill_requests,
)


def command_plan(
    year: int, first_month: int, *, smoke: bool = False, force: bool = False
) -> list[list[str]]:
    requests = (
        (MonthlyRunRequest(year, first_month, force),)
        if smoke
        else sequential_backfill_requests(year, first_month, force=force)
    )
    return [
        [
            "airflow",
            "dags",
            "trigger",
            "nyc_hvfhs_monthly",
            "--conf",
            json.dumps(
                {"year": request.year, "month": request.month, "force": request.force}
            ),
        ]
        for request in requests
    ]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", type=int, default=1)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Reserved for an approved remote runner; never used locally.",
    )
    args = parser.parse_args()
    if args.execute:
        raise SystemExit(
            "E2E execution is intentionally disabled in this repository pass."
        )
    print(
        json.dumps(
            {
                "profile": "smoke" if args.smoke else "release",
                "commands": command_plan(
                    args.year, args.month, smoke=args.smoke, force=args.force
                ),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
