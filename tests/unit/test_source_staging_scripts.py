from __future__ import annotations

from pathlib import Path
import subprocess
import sys

import pytest

from scripts.fetch_source import FREE_SPACE_RESERVE_BYTES, required_free_space
from scripts.upload_release_dataset import UploadObject, _upload_immutable, upload_plan


FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "nyc_hvfhs"
ROOT = Path(__file__).parents[2]


class FakeClientError(Exception):
    def __init__(self, code: str):
        self.response = {"Error": {"Code": code}}


class FakeS3:
    class exceptions:
        ClientError = FakeClientError

    def __init__(self, existing=None):
        self.existing = existing
        self.uploads = []

    def head_object(self, **_):
        if self.existing is None:
            raise FakeClientError("404")
        return self.existing

    def upload_file(self, path, bucket, key, ExtraArgs):
        self.uploads.append((path, bucket, key, ExtraArgs))
        source = Path(path)
        self.existing = {
            "ContentLength": source.stat().st_size,
            "Metadata": ExtraArgs["Metadata"],
        }


def test_upload_plan_uses_landing_and_pins_local_identity(tmp_path: Path) -> None:
    (tmp_path / "fhvhv_tripdata_2024-01.parquet").write_bytes(
        (FIXTURE_DIR / "fhvhv_tripdata_2024-01.fixture.json").read_bytes()
    )
    (tmp_path / "taxi_zone_lookup.csv").write_bytes(
        (FIXTURE_DIR / "taxi_zone_lookup.fixture.csv").read_bytes()
    )
    trip, zones = upload_plan(
        bucket="demo-bucket", year=2024, month=1, source_dir=tmp_path
    )
    assert trip.uri == "s3://demo-bucket/landing/fhvhv_tripdata_2024-01.parquet"
    assert zones.uri == "s3://demo-bucket/reference/taxi_zone_lookup.csv"
    assert len(trip.sha256) == 64 and trip.size_bytes > 0


def test_upload_is_idempotent_and_rejects_changed_object(tmp_path: Path) -> None:
    source = tmp_path / "fhvhv_tripdata_2024-01.parquet"
    source.write_bytes(b"source")
    upload = UploadObject(str(source), "bucket", "landing/file", 6, "abc")
    client = FakeS3()
    assert _upload_immutable(client, upload) == "uploaded"
    assert _upload_immutable(client, upload) == "already-present"
    client.existing = {"ContentLength": 7, "Metadata": {"sha256": "changed"}}
    with pytest.raises(ValueError, match="Refusing to replace"):
        _upload_immutable(client, upload)


def test_download_capacity_keeps_an_operating_system_reserve() -> None:
    assert required_free_space(100) == 100 + FREE_SPACE_RESERVE_BYTES
    with pytest.raises(ValueError, match="cannot be negative"):
        required_free_space(-1)


@pytest.mark.parametrize(
    "module",
    ("scripts.fetch_source", "scripts.upload_release_dataset", "scripts.run_e2e"),
)
def test_operator_scripts_are_module_executable(module: str) -> None:
    result = subprocess.run(
        [sys.executable, "-m", module, "--help"],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
