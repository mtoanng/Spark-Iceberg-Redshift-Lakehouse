"""Download one official NYC TLC month and Taxi Zone lookup to local staging."""

from __future__ import annotations

import argparse
from pathlib import Path
import shutil
from urllib.request import Request, urlopen

from etl.sources.nyc_hvfhs import (
    NYC_TLC_TAXI_ZONE_URI,
    inspect_local_source,
    monthly_trip_filename,
    monthly_trip_uri,
)


FREE_SPACE_RESERVE_BYTES = 256 * 1024 * 1024


def required_free_space(content_length: int) -> int:
    """Keep room for the source plus a small operating-system reserve."""

    if content_length < 0:
        raise ValueError("content_length cannot be negative")
    return content_length + FREE_SPACE_RESERVE_BYTES


def download(url: str, destination: Path, *, overwrite: bool = False) -> Path:
    """Stream one immutable source to a temporary file, then rename it."""

    if destination.exists() and not overwrite:
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_suffix(destination.suffix + ".part")
    request = Request(url, headers={"User-Agent": "nyc-hvfhs-lakehouse/1.0"})
    try:
        with urlopen(request, timeout=60) as response:
            expected = response.headers.get("Content-Length")
            if expected is not None:
                expected_bytes = int(expected)
                free_bytes = shutil.disk_usage(destination.parent).free
                required_bytes = required_free_space(expected_bytes)
                if free_bytes < required_bytes:
                    raise OSError(
                        f"Insufficient free space for {url}: need at least "
                        f"{required_bytes} bytes including reserve, have {free_bytes}."
                    )
            with partial.open("wb") as output:
                shutil.copyfileobj(response, output, length=1024 * 1024)
        if expected is not None and partial.stat().st_size != int(expected):
            raise OSError(
                f"Incomplete download for {url}: expected {expected} bytes, "
                f"received {partial.stat().st_size}."
            )
        partial.replace(destination)
    except Exception:
        partial.unlink(missing_ok=True)
        raise
    return destination


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", type=int, default=1)
    parser.add_argument("--output-dir", type=Path, default=Path("data"))
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    trip_path = download(
        monthly_trip_uri(args.year, args.month),
        args.output_dir / monthly_trip_filename(args.year, args.month),
        overwrite=args.overwrite,
    )
    zone_path = download(
        NYC_TLC_TAXI_ZONE_URI,
        args.output_dir / "taxi_zone_lookup.csv",
        overwrite=args.overwrite,
    )
    trip = inspect_local_source(trip_path, args.year, args.month)
    zone = inspect_local_source(zone_path, args.year, args.month)
    print(
        f"trip={trip_path} bytes={trip.source_size_bytes} sha256={trip.source_checksum}"
    )
    print(
        f"zones={zone_path} bytes={zone.source_size_bytes} sha256={zone.source_checksum}"
    )


if __name__ == "__main__":
    main()
