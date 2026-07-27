"""Fail CI when generated state, source data, or obvious credentials are tracked."""

from __future__ import annotations

from pathlib import Path
import re
import subprocess


ROOT = Path(__file__).parents[1]
FORBIDDEN_TRACKED_PARTS = {
    ".env",
    ".terraform",
    ".venv",
    "__pycache__",
    "data",
    "dbt_packages",
    "logs",
    "metastore_db",
    "target",
}
FORBIDDEN_SUFFIXES = {".tfplan", ".tfstate", ".pyc", ".duckdb", ".parquet"}
AWS_ACCESS_KEY = re.compile(rb"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
REQUIRED_TRACKABLE = (
    "scripts/package_spark_jobs.py",
    "scripts/bootstrap_airflow_runner.sh",
    "scripts/check_repository_hygiene.py",
)


def _git_paths(*args: str) -> tuple[Path, ...]:
    result = subprocess.run(
        ["git", "-c", f"safe.directory={ROOT.as_posix()}", *args],
        cwd=ROOT,
        check=True,
        capture_output=True,
    )
    return tuple(Path(item.decode("utf-8")) for item in result.stdout.split(b"\0") if item)


def violations() -> list[str]:
    tracked = _git_paths("ls-files", "-z")
    problems: list[str] = []
    for relative in tracked:
        path = ROOT / relative
        # A deleted tracked file is a pending cleanup in a developer worktree;
        # it will be absent from `git ls-files` after the change is committed.
        if not path.exists():
            continue
        lowered_parts = {part.lower() for part in relative.parts}
        if lowered_parts & FORBIDDEN_TRACKED_PARTS or relative.suffix.lower() in FORBIDDEN_SUFFIXES:
            problems.append(f"generated or sensitive artifact is tracked: {relative.as_posix()}")
            continue
        if path.is_file() and AWS_ACCESS_KEY.search(path.read_bytes()):
            problems.append(f"possible AWS access key is tracked: {relative.as_posix()}")

    ignored = set(_git_paths("ls-files", "-z", "--others", "--ignored", "--exclude-standard"))
    for required in REQUIRED_TRACKABLE:
        if Path(required) in ignored:
            problems.append(f"required source is ignored: {required}")
        if not (ROOT / required).is_file():
            problems.append(f"required source is missing: {required}")
    return problems


def main() -> None:
    problems = violations()
    if problems:
        raise SystemExit("Repository hygiene failed:\n- " + "\n- ".join(problems))
    print("PASS repository hygiene")


if __name__ == "__main__":
    main()
