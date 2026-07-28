"""Contracts for the atomic dbt build artifact handoff."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from etl.orchestration.nyc_hvfhs_dbt import (
    archive_dbt_run_results,
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
            error.response = {"Error": {"Code": "404"}}
            raise
        return {"ContentLength": len(item["Body"]), "Metadata": item["Metadata"]}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)]["Body"])}


def _run_results(status: str = "success", *, result_count: int = 1) -> bytes:
    return json.dumps(
        {
            "metadata": {"invocation_id": "dbt-invocation"},
            "results": [
                {"status": status, "unique_id": f"model.gold.model_{index}"}
                for index in range(result_count)
            ],
        }
    ).encode()


def test_dbt_build_archives_and_downstream_task_requires_result(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "target" / "run_results.json"
    artifact.parent.mkdir()
    artifact.write_bytes(_run_results())
    s3 = FakeS3()

    uri = archive_dbt_run_results(
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


def test_dbt_build_accepts_multiple_successful_results(tmp_path: Path) -> None:
    artifact = tmp_path / "target" / "run_results.json"
    artifact.parent.mkdir()
    artifact.write_bytes(_run_results(result_count=6))

    uri = archive_dbt_run_results(
        tmp_path,
        publication_prefix_uri="s3://example/manifests",
        source_year=2024,
        source_month=1,
        run_id="stable-run",
        s3_client=FakeS3(),
    )

    assert uri.endswith("/stable-run.json")


def test_dbt_build_rejects_failed_or_incomplete_run_results(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "target" / "run_results.json"
    artifact.parent.mkdir()
    artifact.write_bytes(_run_results("error"))

    with pytest.raises(ValueError, match="non-success"):
        archive_dbt_run_results(
            tmp_path,
            publication_prefix_uri="s3://example/manifests",
            source_year=2024,
            source_month=1,
            run_id="stable-run",
            s3_client=FakeS3(),
        )
