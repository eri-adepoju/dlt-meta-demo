"""
Setup script for DLT-META People Demo.

Creates a UC volume, uploads source data + config files, and renders onboarding
JSON templates with volume paths.

Usage:
    python setup.py --profile test --target dev
"""

import argparse
import io
import json
import os

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

TEMPLATES = [
    "conf/onboarding_bronze_silver_people.template",
    "conf/onboarding_silver_fanout_people.template",
]


def read_bundle_config():
    """Read bundle name and target variables from databricks.yml."""
    with open(os.path.join(SCRIPT_DIR, "databricks.yml")) as f:
        return yaml.safe_load(f)


def create_volume(ws, catalog, schema):
    """Create a managed volume for source data."""
    volume_name = "dlt_meta_files"
    try:
        ws.volumes.read(f"{catalog}.{schema}.{volume_name}")
        print(f"Volume {catalog}.{schema}.{volume_name} already exists.")
    except Exception:
        ws.volumes.create(
            catalog_name=catalog,
            schema_name=schema,
            name=volume_name,
            volume_type=VolumeType.MANAGED,
        )
        print(f"Created volume {catalog}.{schema}.{volume_name}")

    return f"/Volumes/{catalog}/{schema}/{volume_name}/"


def upload_source_data(ws, uc_volume_path):
    """Upload only source data files to the UC volume."""
    data_dir = os.path.join(SCRIPT_DIR, "data")
    for root, _, files in os.walk(data_dir):
        for fname in files:
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, SCRIPT_DIR)
            remote_path = f"{uc_volume_path}{rel_path}"
            with open(local_path, "rb") as f:
                ws.files.upload(file_path=remote_path, contents=f, overwrite=True)
            print(f"  Uploaded {rel_path} -> {remote_path}")


def render_and_upload_onboarding_jsons(ws, catalog, schema, uc_volume_path):
    """Render .template files into .json with real paths, then upload to volume."""
    subs = {
        "{uc_catalog_name}": catalog,
        "{bronze_schema}": schema,
        "{silver_schema}": schema,
        "{uc_volume_path}": uc_volume_path,
    }

    for template_rel in TEMPLATES:
        template_path = os.path.join(SCRIPT_DIR, template_rel)
        out_path = template_path.replace(".template", ".json")

        with open(template_path) as f:
            content = f.read()

        for token, value in subs.items():
            content = content.replace(token, value)

        rendered = json.dumps(json.loads(content), indent=4)

        with open(out_path, "w") as f:
            f.write(rendered)

        # Upload rendered JSON to volume
        rel_path = os.path.relpath(out_path, SCRIPT_DIR)
        remote_path = f"{uc_volume_path}{rel_path}"
        ws.files.upload(file_path=remote_path, contents=io.BytesIO(rendered.encode()), overwrite=True)
        print(f"  Generated + uploaded {rel_path} -> {remote_path}")


def upload_config_files(ws, uc_volume_path):
    """Upload DQE and silver transformation config files to the UC volume."""
    config_files = [
        "conf/dqe/dqe_bronze_people.json",
        "conf/dqe/dqe_silver_people.json",
        "conf/silver_queries_people.json",
    ]
    for rel_path in config_files:
        local_path = os.path.join(SCRIPT_DIR, rel_path)
        remote_path = f"{uc_volume_path}{rel_path}"
        with open(local_path, "rb") as f:
            ws.files.upload(file_path=remote_path, contents=f, overwrite=True)
        print(f"  Uploaded {rel_path} -> {remote_path}")


def main():
    parser = argparse.ArgumentParser(description="Setup DLT-META People Demo")
    parser.add_argument("--profile", required=True, help="Databricks CLI profile")
    parser.add_argument("--target", default="dev", help="Bundle target (default: dev)")
    args = parser.parse_args()

    bundle_cfg = read_bundle_config()
    target_cfg = bundle_cfg["targets"][args.target]
    catalog = target_cfg["variables"]["catalog_name"]
    schema = target_cfg["variables"]["schema_name"]

    ws = WorkspaceClient(profile=args.profile)

    uc_volume_path = create_volume(ws, catalog, schema)

    print(f"\nTarget:         {args.target}")
    print(f"Catalog/Schema: {catalog}.{schema}")
    print(f"Volume:         {uc_volume_path}\n")

    upload_source_data(ws, uc_volume_path)
    upload_config_files(ws, uc_volume_path)
    render_and_upload_onboarding_jsons(ws, catalog, schema, uc_volume_path)

    print(f"\nDone. Next:")
    print(f"  databricks bundle deploy --target {args.target} --profile {args.profile}")
    print(f"  databricks bundle run onboard_people -t {args.target} --profile {args.profile}")
    print(f"  databricks bundle run execute_pipelines -t {args.target} --profile {args.profile}")


if __name__ == "__main__":
    main()
