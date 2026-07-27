"""Read-only checks for the bounded teardown; never deletes resources."""

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
        for service in ("athena", "ec2", "emr-serverless", "glue", "iam", "s3")
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

    try:
        applications = (
            clients["emr-serverless"].list_applications().get("applications", [])
        )
        result["emr_serverless_application"] = (
            "PRESENT"
            if any(app.get("name") == f"{prefix}-spark" for app in applications)
            else "ABSENT"
        )
    except clients["emr-serverless"].exceptions.ClientError as error:
        result["emr_serverless_application"] = "ABSENT" if _absent(error) else "UNKNOWN"

    databases = {}
    for name in ("bronze", "silver", "ops", "gold"):
        try:
            clients["glue"].get_database(Name=name)
            databases[name] = "PRESENT"
        except clients["glue"].exceptions.EntityNotFoundException:
            databases[name] = "ABSENT"
    result["canonical_databases"] = databases

    instances = clients["ec2"].describe_instances(
        Filters=[
            {"Name": "tag:Name", "Values": [f"{prefix}-airflow-runner"]},
            {
                "Name": "instance-state-name",
                "Values": ["pending", "running", "stopping", "stopped"],
            },
        ]
    )
    result["airflow_runner"] = (
        "PRESENT"
        if any(item["Instances"] for item in instances["Reservations"])
        else "ABSENT"
    )

    for kind, api_name, name in (
        ("airflow_role", "get_role", f"{prefix}-airflow-runner"),
        (
            "airflow_instance_profile",
            "get_instance_profile",
            f"{prefix}-airflow-profile",
        ),
        ("emr_serverless_role", "get_role", f"{prefix}-emr-serverless"),
    ):
        argument = "RoleName" if api_name == "get_role" else "InstanceProfileName"
        try:
            getattr(clients["iam"], api_name)(**{argument: name})
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
        result["airflow_runner"],
        result["airflow_role"],
        result["airflow_instance_profile"],
        result["emr_serverless_role"],
        result["emr_serverless_application"],
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
