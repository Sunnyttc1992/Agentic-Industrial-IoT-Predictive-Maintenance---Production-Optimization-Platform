"""Build analytics-ready Gold aggregates from validated Silver telemetry.

This batch pipeline follows the repository's local Pandas/Parquet convention:

* source: ``data/silver/telemetry_clean.parquet``
* destination: ``data/gold``
* quality contract: only rows with ``data_quality_pass = true`` are curated
* outputs: daily maintenance KPIs, hourly production KPIs, and a run manifest

Production counts and OEE are deliberately not calculated because the Silver
contract does not contain good-unit, total-unit, or planned-production fields.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SILVER_FILE = (
    PROJECT_ROOT / "data" / "silver" / "telemetry_clean.parquet"
)
DEFAULT_GOLD_FOLDER = PROJECT_ROOT / "data" / "gold"

MAINTENANCE_FILE_NAME = "maintenance_daily.parquet"
PRODUCTION_FILE_NAME = "production_hourly.parquet"
MANIFEST_FILE_NAME = "gold_pipeline_manifest.json"

SENSOR_COLUMNS = [
    "temperature_c",
    "vibration_mm_s",
    "current_amp",
    "pressure_bar",
    "rpm",
]

REQUIRED_COLUMNS = {
    "machine_id",
    "timestamp",
    "status",
    "fault_type",
    "is_fault",
    "data_quality_pass",
    *SENSOR_COLUMNS,
}

DEFAULT_ACTIVE_STATUSES = frozenset(
    {"RUNNING", "ACTIVE", "OPERATING", "UP"}
)


def _coerce_boolean(series: pd.Series, column_name: str) -> pd.Series:
    """Coerce common boolean representations and reject ambiguous values."""
    boolean_map = {
        True: True,
        False: False,
        1: True,
        0: False,
        1.0: True,
        0.0: False,
        "true": True,
        "false": False,
        "1": True,
        "0": False,
        "yes": True,
        "no": False,
    }

    normalized = series.map(
        lambda value: value.strip().lower()
        if isinstance(value, str)
        else value
    )
    coerced = normalized.map(boolean_map)

    invalid = series.notna() & coerced.isna()
    if invalid.any():
        examples = series.loc[invalid].astype(str).unique()[:5].tolist()
        raise ValueError(
            f"Column '{column_name}' contains invalid boolean values: {examples}"
        )

    return coerced.astype("boolean")


def validate_silver_schema(df: pd.DataFrame) -> None:
    """Validate the Silver columns needed by both Gold data products."""
    missing_columns = sorted(REQUIRED_COLUMNS.difference(df.columns))
    if missing_columns:
        raise ValueError(
            f"Silver telemetry is missing required columns: {missing_columns}"
        )

    if df.empty:
        raise ValueError("Silver telemetry is empty; no Gold tables can be built.")


def prepare_silver_telemetry(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the Gold input contract and return deterministic, valid records."""
    validate_silver_schema(df)
    prepared = df.copy()

    prepared["timestamp"] = pd.to_datetime(
        prepared["timestamp"], utc=True, errors="coerce"
    )
    prepared["data_quality_pass"] = _coerce_boolean(
        prepared["data_quality_pass"], "data_quality_pass"
    )
    prepared["is_fault"] = _coerce_boolean(
        prepared["is_fault"], "is_fault"
    )

    for column in SENSOR_COLUMNS:
        prepared[column] = pd.to_numeric(prepared[column], errors="coerce")

    prepared["machine_id"] = prepared["machine_id"].astype("string").str.strip()
    prepared["status"] = prepared["status"].astype("string").str.strip().str.upper()
    prepared["fault_type"] = (
        prepared["fault_type"].astype("string").str.strip().str.upper()
    )

    prepared = prepared.loc[prepared["data_quality_pass"].fillna(False)].copy()

    null_contract_columns = [
        "machine_id",
        "timestamp",
        "status",
        "fault_type",
        "is_fault",
        *SENSOR_COLUMNS,
    ]
    null_rows = prepared[null_contract_columns].isna().any(axis=1)
    if null_rows.any():
        raise ValueError(
            "Quality-passing Silver rows contain null contract fields: "
            f"{int(null_rows.sum())} row(s)."
        )

    if (prepared["machine_id"] == "").any():
        raise ValueError("Quality-passing Silver rows contain blank machine IDs.")

    if prepared.empty:
        raise ValueError(
            "Silver telemetry contains no rows where data_quality_pass is true."
        )

    prepared["is_fault"] = prepared["is_fault"].astype(bool)
    prepared = prepared.drop_duplicates(
        subset=["machine_id", "timestamp"], keep="last"
    )
    prepared = prepared.sort_values(
        ["machine_id", "timestamp"]
    ).reset_index(drop=True)

    return prepared


def _percent(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Return a percentage, leaving zero-denominator rows undefined."""
    result = numerator.div(denominator.where(denominator.ne(0))).mul(100)
    return result.round(2)


def _normalize_active_statuses(active_statuses: Iterable[str]) -> set[str]:
    """Normalize and validate configured active machine statuses."""
    normalized = {status.strip().upper() for status in active_statuses}
    if not normalized:
        raise ValueError("At least one active status must be configured.")
    return normalized


def build_maintenance_daily(
    df: pd.DataFrame,
    active_statuses: Iterable[str] = DEFAULT_ACTIVE_STATUSES,
) -> pd.DataFrame:
    """Aggregate validated readings to one maintenance row per machine/day."""
    maintenance = df.copy()
    maintenance["metric_date"] = maintenance["timestamp"].dt.floor("D")
    maintenance["is_active"] = maintenance["status"].isin(
        _normalize_active_statuses(active_statuses)
    )

    previous_fault = maintenance.groupby("machine_id")["is_fault"].shift(
        fill_value=False
    )
    maintenance["fault_event_start"] = (
        maintenance["is_fault"] & ~previous_fault
    )

    fault_rows = maintenance.loc[maintenance["is_fault"]].copy()
    fault_rows["fault_type"] = fault_rows["fault_type"].where(
        fault_rows["fault_type"].ne("NORMAL"), "UNSPECIFIED"
    )

    group_keys = ["machine_id", "metric_date"]
    aggregates = maintenance.groupby(group_keys, as_index=False).agg(
        first_reading_at=("timestamp", "min"),
        last_reading_at=("timestamp", "max"),
        telemetry_reading_count=("timestamp", "size"),
        active_reading_count=("is_active", "sum"),
        fault_reading_count=("is_fault", "sum"),
        fault_event_count=("fault_event_start", "sum"),
        avg_temperature_c=("temperature_c", "mean"),
        p95_temperature_c=("temperature_c", lambda values: values.quantile(0.95)),
        max_temperature_c=("temperature_c", "max"),
        avg_vibration_mm_s=("vibration_mm_s", "mean"),
        p95_vibration_mm_s=(
            "vibration_mm_s", lambda values: values.quantile(0.95)
        ),
        max_vibration_mm_s=("vibration_mm_s", "max"),
        avg_current_amp=("current_amp", "mean"),
        max_current_amp=("current_amp", "max"),
        avg_pressure_bar=("pressure_bar", "mean"),
        min_pressure_bar=("pressure_bar", "min"),
        avg_rpm=("rpm", "mean"),
    )

    fault_summary = fault_rows.groupby(group_keys, as_index=False).agg(
        distinct_fault_type_count=("fault_type", "nunique"),
        last_fault_at=("timestamp", "max"),
    )

    if fault_rows.empty:
        dominant_fault = pd.DataFrame(
            columns=[*group_keys, "dominant_fault_type"]
        )
    else:
        fault_counts = (
            fault_rows.groupby([*group_keys, "fault_type"], as_index=False)
            .size()
            .sort_values(
                [*group_keys, "size", "fault_type"],
                ascending=[True, True, False, True],
            )
        )
        dominant_fault = (
            fault_counts.drop_duplicates(group_keys, keep="first")
            .rename(columns={"fault_type": "dominant_fault_type"})[
                [*group_keys, "dominant_fault_type"]
            ]
        )

    result = aggregates.merge(fault_summary, on=group_keys, how="left")
    result = result.merge(dominant_fault, on=group_keys, how="left")

    result["active_reading_pct"] = _percent(
        result["active_reading_count"], result["telemetry_reading_count"]
    )
    result["fault_reading_pct"] = _percent(
        result["fault_reading_count"], result["telemetry_reading_count"]
    )
    result["distinct_fault_type_count"] = (
        result["distinct_fault_type_count"].fillna(0).astype("int64")
    )
    result["dominant_fault_type"] = result["dominant_fault_type"].fillna(
        "NONE"
    )
    result["period_end"] = result["metric_date"] + pd.Timedelta(days=1)

    integer_columns = [
        "telemetry_reading_count",
        "active_reading_count",
        "fault_reading_count",
        "fault_event_count",
    ]
    result[integer_columns] = result[integer_columns].astype("int64")

    ordered_columns = [
        "machine_id",
        "metric_date",
        "period_end",
        "first_reading_at",
        "last_reading_at",
        "telemetry_reading_count",
        "active_reading_count",
        "active_reading_pct",
        "fault_reading_count",
        "fault_reading_pct",
        "fault_event_count",
        "distinct_fault_type_count",
        "dominant_fault_type",
        "last_fault_at",
        "avg_temperature_c",
        "p95_temperature_c",
        "max_temperature_c",
        "avg_vibration_mm_s",
        "p95_vibration_mm_s",
        "max_vibration_mm_s",
        "avg_current_amp",
        "max_current_amp",
        "avg_pressure_bar",
        "min_pressure_bar",
        "avg_rpm",
    ]

    return result[ordered_columns].sort_values(group_keys).reset_index(drop=True)


def build_production_hourly(
    df: pd.DataFrame,
    max_observation_gap_seconds: float = 300.0,
    active_statuses: Iterable[str] = DEFAULT_ACTIVE_STATUSES,
) -> pd.DataFrame:
    """Aggregate telemetry to hourly runtime and production-proxy KPIs.

    Runtime uses the interval until a machine's next observation. Intervals are
    capped so outages or missing telemetry are not mislabeled as machine time.
    The final observation in a sequence contributes a reading but zero seconds.
    """
    if max_observation_gap_seconds <= 0:
        raise ValueError("max_observation_gap_seconds must be greater than zero.")

    production = df.copy()
    normalized_active_statuses = _normalize_active_statuses(active_statuses)

    production["window_start"] = production["timestamp"].dt.floor("h")
    production["window_end"] = production["window_start"] + pd.Timedelta(
        hours=1
    )
    production["is_active"] = production["status"].isin(
        normalized_active_statuses
    )

    next_timestamp = production.groupby("machine_id")["timestamp"].shift(-1)
    seconds_to_next = (next_timestamp - production["timestamp"]).dt.total_seconds()
    seconds_to_window_end = (
        production["window_end"] - production["timestamp"]
    ).dt.total_seconds()

    production["covered_seconds"] = (
        pd.concat(
            [
                seconds_to_next,
                seconds_to_window_end,
                pd.Series(
                    max_observation_gap_seconds,
                    index=production.index,
                    dtype="float64",
                ),
            ],
            axis=1,
        )
        .min(axis=1)
        .clip(lower=0)
        .fillna(0.0)
        .where(next_timestamp.notna(), 0.0)
    )
    production["estimated_runtime_seconds"] = production[
        "covered_seconds"
    ].where(production["is_active"], 0.0)
    production["estimated_downtime_seconds"] = production[
        "covered_seconds"
    ].where(~production["is_active"], 0.0)
    production["estimated_revolutions"] = (
        production["rpm"]
        * production["estimated_runtime_seconds"]
        / 60.0
    )

    group_keys = ["machine_id", "window_start", "window_end"]
    result = production.groupby(group_keys, as_index=False).agg(
        first_reading_at=("timestamp", "min"),
        last_reading_at=("timestamp", "max"),
        telemetry_reading_count=("timestamp", "size"),
        active_reading_count=("is_active", "sum"),
        fault_reading_count=("is_fault", "sum"),
        covered_seconds=("covered_seconds", "sum"),
        estimated_runtime_seconds=("estimated_runtime_seconds", "sum"),
        estimated_downtime_seconds=("estimated_downtime_seconds", "sum"),
        estimated_revolutions=("estimated_revolutions", "sum"),
        avg_rpm=("rpm", "mean"),
        min_rpm=("rpm", "min"),
        max_rpm=("rpm", "max"),
        avg_current_amp=("current_amp", "mean"),
        avg_pressure_bar=("pressure_bar", "mean"),
    )

    result["availability_pct"] = _percent(
        result["estimated_runtime_seconds"], result["covered_seconds"]
    )
    result["fault_reading_pct"] = _percent(
        result["fault_reading_count"], result["telemetry_reading_count"]
    )

    integer_columns = [
        "telemetry_reading_count",
        "active_reading_count",
        "fault_reading_count",
    ]
    result[integer_columns] = result[integer_columns].astype("int64")

    duration_columns = [
        "covered_seconds",
        "estimated_runtime_seconds",
        "estimated_downtime_seconds",
    ]
    result[duration_columns] = result[duration_columns].round(3)
    result["estimated_revolutions"] = result["estimated_revolutions"].round(2)

    ordered_columns = [
        "machine_id",
        "window_start",
        "window_end",
        "first_reading_at",
        "last_reading_at",
        "telemetry_reading_count",
        "active_reading_count",
        "fault_reading_count",
        "fault_reading_pct",
        "covered_seconds",
        "estimated_runtime_seconds",
        "estimated_downtime_seconds",
        "availability_pct",
        "estimated_revolutions",
        "avg_rpm",
        "min_rpm",
        "max_rpm",
        "avg_current_amp",
        "avg_pressure_bar",
    ]

    return result[ordered_columns].sort_values(group_keys).reset_index(drop=True)


def validate_gold_outputs(
    maintenance_df: pd.DataFrame,
    production_df: pd.DataFrame,
    accepted_row_count: int,
) -> None:
    """Fail the run when Gold grain, reconciliation, or range checks fail."""
    if maintenance_df.empty or production_df.empty:
        raise ValueError("Gold output tables must not be empty.")

    output_checks = [
        (
            maintenance_df,
            ["machine_id", "metric_date"],
            "maintenance_daily",
        ),
        (
            production_df,
            ["machine_id", "window_start"],
            "production_hourly",
        ),
    ]
    for output_df, key_columns, table_name in output_checks:
        if output_df[key_columns].isna().any(axis=None):
            raise ValueError(f"{table_name} contains a null primary key.")
        if output_df.duplicated(key_columns).any():
            raise ValueError(f"{table_name} contains duplicate primary keys.")

        percentage_columns = [
            column
            for column in output_df.columns
            if column.endswith("_pct")
        ]
        for column in percentage_columns:
            values = output_df[column].dropna()
            if not values.between(0, 100, inclusive="both").all():
                raise ValueError(
                    f"{table_name}.{column} contains values outside 0-100."
                )

    for table_name, output_df in (
        ("maintenance_daily", maintenance_df),
        ("production_hourly", production_df),
    ):
        reading_count = int(output_df["telemetry_reading_count"].sum())
        if reading_count != accepted_row_count:
            raise ValueError(
                f"{table_name} reconciles to {reading_count} readings; "
                f"expected {accepted_row_count}."
            )

    duration_columns = [
        "covered_seconds",
        "estimated_runtime_seconds",
        "estimated_downtime_seconds",
    ]
    if (production_df[duration_columns] < 0).any(axis=None):
        raise ValueError("production_hourly contains negative duration values.")

    reconciled_duration = (
        production_df["estimated_runtime_seconds"]
        + production_df["estimated_downtime_seconds"]
    ).round(3)
    if not reconciled_duration.equals(
        production_df["covered_seconds"].round(3)
    ):
        raise ValueError(
            "production_hourly runtime and downtime do not reconcile to coverage."
        )


def run_pipeline(
    silver_file: Path = DEFAULT_SILVER_FILE,
    gold_folder: Path = DEFAULT_GOLD_FOLDER,
    max_observation_gap_seconds: float = 300.0,
    active_statuses: Iterable[str] = DEFAULT_ACTIVE_STATUSES,
) -> dict[str, object]:
    """Read Silver, build both Gold tables, write outputs, and return metadata."""
    silver_file = Path(silver_file)
    gold_folder = Path(gold_folder)

    if not silver_file.is_file():
        raise FileNotFoundError(f"Silver telemetry file not found: {silver_file}")

    source_df = pd.read_parquet(silver_file)
    prepared_df = prepare_silver_telemetry(source_df)
    normalized_active_statuses = _normalize_active_statuses(active_statuses)
    maintenance_df = build_maintenance_daily(
        prepared_df, active_statuses=normalized_active_statuses
    )
    production_df = build_production_hourly(
        prepared_df,
        max_observation_gap_seconds=max_observation_gap_seconds,
        active_statuses=normalized_active_statuses,
    )
    validate_gold_outputs(
        maintenance_df,
        production_df,
        accepted_row_count=len(prepared_df),
    )

    gold_folder.mkdir(parents=True, exist_ok=True)
    maintenance_file = gold_folder / MAINTENANCE_FILE_NAME
    production_file = gold_folder / PRODUCTION_FILE_NAME
    manifest_file = gold_folder / MANIFEST_FILE_NAME

    maintenance_df.to_parquet(maintenance_file, index=False)
    production_df.to_parquet(production_file, index=False)

    generated_at = pd.Timestamp.now(tz="UTC")
    active_status_list = sorted(normalized_active_statuses)
    manifest: dict[str, object] = {
        "pipeline": "silver_to_gold",
        "generated_at": generated_at.isoformat(),
        "source_file": str(silver_file.resolve()),
        "source_row_count": int(len(source_df)),
        "accepted_row_count": int(len(prepared_df)),
        "rejected_row_count": int(len(source_df) - len(prepared_df)),
        "machine_count": int(prepared_df["machine_id"].nunique()),
        "source_min_timestamp": prepared_df["timestamp"].min().isoformat(),
        "source_max_timestamp": prepared_df["timestamp"].max().isoformat(),
        "active_statuses": active_status_list,
        "max_observation_gap_seconds": max_observation_gap_seconds,
        "quality_checks": {
            "status": "passed",
            "checks": [
                "non_empty_outputs",
                "unique_non_null_primary_keys",
                "accepted_row_reconciliation",
                "percentage_ranges",
                "production_duration_reconciliation",
            ],
        },
        "outputs": {
            "maintenance_daily": {
                "file": str(maintenance_file.resolve()),
                "row_count": int(len(maintenance_df)),
            },
            "production_hourly": {
                "file": str(production_file.resolve()),
                "row_count": int(len(production_df)),
            },
        },
    }

    with manifest_file.open("w", encoding="utf-8") as file:
        json.dump(manifest, file, indent=2)
        file.write("\n")

    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Gold maintenance and production tables from Silver telemetry."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_SILVER_FILE,
        help=f"Silver Parquet file (default: {DEFAULT_SILVER_FILE})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_GOLD_FOLDER,
        help=f"Gold output directory (default: {DEFAULT_GOLD_FOLDER})",
    )
    parser.add_argument(
        "--max-observation-gap-seconds",
        type=float,
        default=300.0,
        help="Maximum interval credited to runtime/downtime (default: 300).",
    )
    parser.add_argument(
        "--active-status",
        action="append",
        dest="active_statuses",
        help=(
            "Status treated as active; repeat for multiple values. "
            "Defaults to RUNNING, ACTIVE, OPERATING, and UP."
        ),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    active_statuses = args.active_statuses or DEFAULT_ACTIVE_STATUSES
    manifest = run_pipeline(
        silver_file=args.input,
        gold_folder=args.output_dir,
        max_observation_gap_seconds=args.max_observation_gap_seconds,
        active_statuses=active_statuses,
    )

    print("Gold pipeline completed successfully.")
    print(
        f"Accepted {manifest['accepted_row_count']} of "
        f"{manifest['source_row_count']} Silver rows."
    )
    for name, output in manifest["outputs"].items():
        print(f"{name}: {output['row_count']} rows -> {output['file']}")


if __name__ == "__main__":
    main()
