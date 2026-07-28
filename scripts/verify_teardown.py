"""Read-only checks for bounded teardown; never deletes resources."""

from __future__ import annotations

import argparse
import json
from typing import Any

import boto3


def _absent(error: Exception) -> bool:
    response = getattr(error, "response", {})
    return response.get("Error", {}).get("Code") in {
        "404",
        "EntityNotFound",
        "EntityNotFoundException",
        "InvalidRequestException",
        "NoSuchBucket",
        "NoSuchEntity",
        "ResourceNotFoundException",
        "ValidationException",
    }


def verify(
    bucket: str,
    workgroup: str,
    *,
    project_name: str,
    environment: str,
    region: str,
    athena_results_prefix: str = "athena-results",
    clients: dict[str, Any] | None = None,
) -> dict[str, object]:
    clients = clients or {
        service: boto3.client(service, region_name=region)
        for service in (
            "athena",
            "emr-serverless",
            "glue",
            "iam",
            "mwaa",
            "redshift-serverless",
            "s3",
        )
    }
    prefix = f"{project_name}-{environment}"
    result: dict[str, object] = {}

    try:
        clients["s3"].head_bucket(Bucket=bucket)
        result["canonical_bucket"] = "PRESENT"
    except clients["s3"].exceptions.ClientError as error:
        result["canonical_bucket"] = "ABSENT" if _absent(error) else "UNKNOWN"

    temporary_prefixes = {}
    for key in ("tmp/", f"{athena_results_prefix.strip('/')}/"):
        objects = clients["s3"].list_objects_v2(Bucket=bucket, Prefix=key, MaxKeys=1)
        temporary_prefixes[key] = (
            "EMPTY" if objects.get("KeyCount", 0) == 0 else "PRESENT"
        )
    result["temporary_prefixes"] = temporary_prefixes

    try:
        clients["athena"].get_work_group(WorkGroup=workgroup)
        result["athena_workgroup"] = "PRESENT"
    except clients["athena"].exceptions.ClientError as error:
        result["athena_workgroup"] = "ABSENT" if _absent(error) else "UNKNOWN"

    applications = clients["emr-serverless"].list_applications().get("applications", [])
    result["emr_serverless_application"] = (
        "PRESENT"
        if any(app.get("name") == f"{prefix}-spark" for app in applications)
        else "ABSENT"
    )

    databases = {}
    for name in ("bronze", "silver", "ops"):
        try:
            clients["glue"].get_database(Name=name)
            databases[name] = "PRESENT"
        except clients["glue"].exceptions.EntityNotFoundException:
            databases[name] = "ABSENT"
    result["canonical_databases"] = databases

    try:
        clients["mwaa"].get_environment(Name=prefix)
        result["mwaa_environment"] = "PRESENT"
    except clients["mwaa"].exceptions.ClientError as error:
        result["mwaa_environment"] = "ABSENT" if _absent(error) else "UNKNOWN"

    try:
        clients["redshift-serverless"].get_workgroup(workgroupName=prefix)
        result["redshift_workgroup"] = "PRESENT"
    except clients["redshift-serverless"].exceptions.ClientError as error:
        result["redshift_workgroup"] = "ABSENT" if _absent(error) else "UNKNOWN"

    for kind, name in (
        ("mwaa_role", f"{prefix}-mwaa"),
        ("emr_serverless_role", f"{prefix}-emr-serverless"),
        ("redshift_spectrum_role", f"{prefix}-redshift-spectrum"),
    ):
        try:
            clients["iam"].get_role(RoleName=name)
            result[kind] = "PRESENT"
        except clients["iam"].exceptions.NoSuchEntityException:
            result[kind] = "ABSENT"
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--workgroup", required=True)
    parser.add_argument("--project-name", default="nyc-hvfhs-lakehouse")
    parser.add_argument("--environment", default="dev")
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--athena-results-prefix", default="athena-results")
    args = parser.parse_args()
    result = verify(
        args.bucket,
        args.workgroup,
        project_name=args.project_name,
        environment=args.environment,
        region=args.region,
        athena_results_prefix=args.athena_results_prefix,
    )
    print(json.dumps(result, indent=2, sort_keys=True))

    expected_absent = [
        result["athena_workgroup"],
        result["mwaa_environment"],
        result["mwaa_role"],
        result["emr_serverless_role"],
        result["emr_serverless_application"],
        result["redshift_workgroup"],
        result["redshift_spectrum_role"],
    ]
    if (
        result["canonical_bucket"] != "PRESENT"
        or any(value != "PRESENT" for value in result["canonical_databases"].values())
        or any(value != "ABSENT" for value in expected_absent)
        or any(value != "EMPTY" for value in result["temporary_prefixes"].values())
    ):
        raise SystemExit(
            "Teardown verification did not reach the expected retained-data state."
        )


if __name__ == "__main__":
    main()
