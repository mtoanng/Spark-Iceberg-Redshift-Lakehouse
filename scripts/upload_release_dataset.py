"""Plan or perform an immutable local-staging to S3 landing upload."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import json
from pathlib import Path
from typing import Any

import boto3

from etl.sources.nyc_hvfhs import inspect_local_source, monthly_trip_filename


@dataclass(frozen=True)
class UploadObject:
    local_path: str
    bucket: str
    key: str
    size_bytes: int
    sha256: str

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.key}"


def upload_plan(
    *,
    bucket: str,
    year: int,
    month: int,
    source_dir: Path,
    landing_prefix: str = "landing",
    reference_prefix: str = "reference",
) -> tuple[UploadObject, UploadObject]:
    trip_path = source_dir / monthly_trip_filename(year, month)
    zone_path = source_dir / "taxi_zone_lookup.csv"
    trip = inspect_local_source(trip_path, year, month)
    zone = inspect_local_source(zone_path, year, month)
    return (
        UploadObject(
            str(trip_path),
            bucket,
            f"{landing_prefix.strip('/')}/{trip_path.name}",
            trip.source_size_bytes,
            trip.source_checksum,
        ),
        UploadObject(
            str(zone_path),
            bucket,
            f"{reference_prefix.strip('/')}/{zone_path.name}",
            zone.source_size_bytes,
            zone.source_checksum,
        ),
    )


def _upload_immutable(client: Any, item: UploadObject) -> str:
    try:
        existing = client.head_object(Bucket=item.bucket, Key=item.key)
    except client.exceptions.ClientError as error:
        code = error.response.get("Error", {}).get("Code")
        if code not in {"404", "NoSuchKey", "NotFound"}:
            raise
    else:
        metadata = existing.get("Metadata", {})
        if (
            int(existing.get("ContentLength", -1)) == item.size_bytes
            and metadata.get("sha256") == item.sha256
        ):
            return "already-present"
        raise ValueError(f"Refusing to replace changed immutable object {item.uri}")

    client.upload_file(
        item.local_path,
        item.bucket,
        item.key,
        ExtraArgs={"Metadata": {"sha256": item.sha256}},
    )
    uploaded = client.head_object(Bucket=item.bucket, Key=item.key)
    if (
        int(uploaded.get("ContentLength", -1)) != item.size_bytes
        or uploaded.get("Metadata", {}).get("sha256") != item.sha256
    ):
        raise ValueError(f"Uploaded object identity verification failed for {item.uri}")
    return "uploaded"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", type=int, default=1)
    parser.add_argument("--source-dir", type=Path, default=Path("data"))
    parser.add_argument("--landing-prefix", default="landing")
    parser.add_argument("--reference-prefix", default="reference")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Upload through the default AWS credential chain; otherwise print a plan.",
    )
    args = parser.parse_args()
    items = upload_plan(
        bucket=args.bucket,
        year=args.year,
        month=args.month,
        source_dir=args.source_dir,
        landing_prefix=args.landing_prefix,
        reference_prefix=args.reference_prefix,
    )
    statuses = ["planned", "planned"]
    if args.execute:
        client = boto3.client("s3", region_name=args.region)
        statuses = [_upload_immutable(client, item) for item in items]

    trip, zones = items
    output = {
        "mode": "execute" if args.execute else "plan",
        "objects": [
            {**asdict(item), "uri": item.uri, "status": status}
            for item, status in zip(items, statuses, strict=True)
        ],
        "airflow_variables": {
            "nyc_landing_uri": f"s3://{args.bucket}/{args.landing_prefix.strip('/')}",
            "nyc_taxi_zone_uri": zones.uri,
            "nyc_taxi_zone_sha256": zones.sha256,
            f"nyc_hvfhs_{args.year}_{args.month:02d}_sha256": trip.sha256,
            f"nyc_hvfhs_{args.year}_{args.month:02d}_size_bytes": trip.size_bytes,
        },
    }
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
