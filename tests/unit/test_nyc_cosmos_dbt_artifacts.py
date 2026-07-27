"""Contracts for the durable Cosmos dbt artifact handoff."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from etl.orchestration.nyc_hvfhs_cosmos import (
    archive_dbt_run_results_for_run,
    require_dbt_result_artifact,
)


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict[str, object]] = {}

    def put_object(self, **kwargs) -> None:
        self.objects[(kwargs["Bucket"], kwargs["Key"])] = kwargs

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        try:
            item = self.objects[(Bucket, Key)]
        except KeyError as error:
            raise RuntimeError("not found") from error
        return {"ContentLength": len(item["Body"]), "Metadata": item["Metadata"]}


def _run_results(status: str = "success") -> bytes:
    return json.dumps(
        {
            "metadata": {"invocation_id": "dbt-invocation"},
            "results": [{"status": status, "unique_id": "model.gold.dim_date"}],
        }
    ).encode()


def test_cosmos_callback_archives_and_downstream_task_requires_result(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "target" / "run_results.json"
    artifact.parent.mkdir()
    artifact.write_bytes(_run_results())
    s3 = FakeS3()

    uri = archive_dbt_run_results_for_run(
        tmp_path,
        publication_prefix_uri="s3://example/manifests/",
        source_year=2024,
        source_month=1,
        run_id="stable-run",
        s3_client=s3,
    )

    assert (
        uri == "s3://example/manifests/dbt-results/year=2024/month=01/stable-run.json"
    )
    assert (
        require_dbt_result_artifact(
            "s3://example/manifests", 2024, 1, "stable-run", s3_client=s3
        )
        == uri
    )
    stored = s3.objects[
        ("example", "manifests/dbt-results/year=2024/month=01/stable-run.json")
    ]
    assert stored["ContentType"] == "application/json"
    assert len(stored["Metadata"]["sha256"]) == 64


def test_cosmos_callback_rejects_failed_or_incomplete_run_results(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "target" / "run_results.json"
    artifact.parent.mkdir()
    artifact.write_bytes(_run_results("error"))

    with pytest.raises(ValueError, match="non-success"):
        archive_dbt_run_results_for_run(
            tmp_path,
            publication_prefix_uri="s3://example/manifests",
            source_year=2024,
            source_month=1,
            run_id="stable-run",
            s3_client=FakeS3(),
        )
