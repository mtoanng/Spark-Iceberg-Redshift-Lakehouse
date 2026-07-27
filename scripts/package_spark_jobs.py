"""Build the deterministic shared Python artifact used by EMR Serverless jobs.

Usage: ``python scripts/package_spark_jobs.py --output build/nyc_spark_jobs.zip``.
The resulting zip is uploaded to ``spark_jobs/nyc_spark_jobs.zip`` and supplied
to EMR Serverless with Spark's ``--py-files`` argument.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo


ROOT = Path(__file__).parents[1]
# EMR Serverless provides boto3; Great Expectations requires deployment image
# verification before an AWS run.
RUNTIME_DEPENDENCIES = {"great_expectations": "1.19.0"}


def _files() -> list[Path]:
    return sorted(
        path
        for path in (ROOT / "etl").rglob("*.py")
        if "dags" not in path.parts and "__pycache__" not in path.parts
    )


def build(output: Path) -> dict[str, object]:
    output.parent.mkdir(parents=True, exist_ok=True)
    entries = [(path, path.relative_to(ROOT).as_posix()) for path in _files()]
    manifest = {
        "artifact": "nyc_spark_jobs",
        "format": "zip",
        "entrypoints": [
            "etl/spark_jobs/initialize_nyc_iceberg_tables.py",
            "etl/spark_jobs/nyc_bronze_ingestion.py",
            "etl/spark_jobs/nyc_great_expectations_checkpoint.py",
            "etl/spark_jobs/nyc_silver_transform.py",
            "etl/spark_jobs/nyc_quality_checkpoint.py",
            "etl/spark_jobs/nyc_publish_manifest.py",
        ],
        "dependencies": RUNTIME_DEPENDENCIES,
        "catalog": "glue_catalog",
        "namespaces": ["bronze", "silver", "ops", "gold"],
        "artifact_s3_key": "spark_jobs/nyc_spark_jobs.zip",
    }
    with ZipFile(output, "w", compression=ZIP_DEFLATED, compresslevel=9) as archive:
        for path, name in entries:
            info = ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = ZIP_DEFLATED
            archive.writestr(info, path.read_bytes())
        info = ZipInfo("spark_runtime_manifest.json", date_time=(1980, 1, 1, 0, 0, 0))
        info.compress_type = ZIP_DEFLATED
        archive.writestr(
            info, json.dumps(manifest, sort_keys=True, separators=(",", ":"))
        )
    manifest["sha256"] = hashlib.sha256(output.read_bytes()).hexdigest()
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("build/nyc_spark_jobs.zip"))
    parser.add_argument(
        "--check", action="store_true", help="Build and validate the artifact contract."
    )
    args = parser.parse_args()
    manifest = build(args.output)
    if args.check:
        with ZipFile(args.output) as archive:
            names = set(archive.namelist())
        missing = [entry for entry in manifest["entrypoints"] if entry not in names]
        if missing or "spark_runtime_manifest.json" not in names:
            raise SystemExit(f"Glue package contract failed; missing={missing}")
    print(json.dumps(manifest, sort_keys=True))


if __name__ == "__main__":
    main()
