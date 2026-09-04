"""Digital-twin application service interface."""
from __future__ import annotations

import argparse
import boto3

MODEL_NAME = "sunny-industrial-press-model"

PROPERTY_NAMES = (
    "temperature_c",
    "vibration_mm_s",
    "current_amp",
    "pressure_bar",
    "rpm",
    "status",
    "anomaly_score",
    "is_anomaly",
    "health_score",
)

EXPECTED_ASSETS = {f"press_{number:02d}" for number in range(1, 11)}


def find_asset_model_id(client):
    paginator = client.get_paginator("list_asset_models")

    for page in paginator.paginate():
        for model in page["assetModelSummaries"]:
            if model["name"] == MODEL_NAME:
                return model["id"]

    raise ValueError(f"Asset model '{MODEL_NAME}' was not found.")


def get_press_assets(client, model_id):
    paginator = client.get_paginator("list_assets")
    assets = {}

    for page in paginator.paginate(assetModelId=model_id):
        for asset in page["assetSummaries"]:
            if asset["name"] in EXPECTED_ASSETS:
                assets[asset["name"]] = asset["id"]

    missing_assets = EXPECTED_ASSETS - set(assets)

    if missing_assets:
        raise ValueError(
            f"Missing SiteWise assets: {', '.join(sorted(missing_assets))}"
        )

    return assets


def configure_aliases(client, assets):
    configured_count = 0

    for asset_name, asset_id in sorted(assets.items()):
        asset = client.describe_asset(assetId=asset_id)

        properties = {
            property_info["name"]: property_info["id"]
            for property_info in asset["assetProperties"]
        }

        for property_name in PROPERTY_NAMES:
            alias = f"/sunny/factory/{asset_name}/{property_name}"

            client.update_asset_property(
                assetId=asset_id,
                propertyId=properties[property_name],
                propertyAlias=alias,
            )

            configured_count += 1
            print(f"Configured: {alias}")

    print(f"\nSuccess: configured {configured_count} property aliases.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--region", required=True)
    parser.add_argument("--profile", default=None)
    args = parser.parse_args()

    session = boto3.Session(
        profile_name=args.profile,
        region_name=args.region,
    )
    client = session.client("iotsitewise")

    model_id = find_asset_model_id(client)
    assets = get_press_assets(client, model_id)

    configure_aliases(client, assets)


if __name__ == "__main__":
    main()
    