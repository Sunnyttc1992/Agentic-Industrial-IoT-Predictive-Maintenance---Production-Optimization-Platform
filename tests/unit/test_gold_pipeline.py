import json

import pandas as pd
import pytest

from src.pipelines.gold_pipeline import (
    MAINTENANCE_FILE_NAME,
    MANIFEST_FILE_NAME,
    PRODUCTION_FILE_NAME,
    build_maintenance_daily,
    build_production_hourly,
    prepare_silver_telemetry,
    run_pipeline,
)


def silver_fixture() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "machine_id": ["press_01"] * 5 + ["press_02"],
            "timestamp": pd.to_datetime(
                [
                    "2026-09-03T10:00:00Z",
                    "2026-09-03T10:01:00Z",
                    "2026-09-03T10:02:00Z",
                    "2026-09-03T10:03:00Z",
                    "2026-09-03T10:04:00Z",
                    "2026-09-03T10:00:00Z",
                ],
                utc=True,
            ),
            "temperature_c": [70.0, 72.0, 75.0, 73.0, 71.0, 65.0],
            "vibration_mm_s": [2.0, 2.1, 5.0, 4.0, 2.2, 1.5],
            "current_amp": [12.0, 12.5, 18.0, 16.0, 13.0, 10.0],
            "pressure_bar": [6.0, 6.1, 5.5, 5.8, 6.0, 7.0],
            "rpm": [3000, 3000, 2000, 0, 3000, 2500],
            "status": [
                "running",
                "running",
                "stopped",
                "stopped",
                "running",
                "running",
            ],
            "fault_type": [
                "NORMAL",
                "NORMAL",
                "OVERHEATING",
                "OVERHEATING",
                "NORMAL",
                "NORMAL",
            ],
            "is_fault": [0, 0, 1, 1, 0, 0],
            "data_quality_pass": [True, True, True, True, False, True],
        }
    )


def test_prepare_silver_filters_quality_rows_and_normalizes_values():
    result = prepare_silver_telemetry(silver_fixture())

    assert len(result) == 5
    assert result["status"].isin({"RUNNING", "STOPPED"}).all()
    assert result["is_fault"].dtype == bool
    assert not result.duplicated(["machine_id", "timestamp"]).any()


def test_maintenance_daily_counts_fault_events_not_only_fault_readings():
    prepared = prepare_silver_telemetry(silver_fixture())
    result = build_maintenance_daily(prepared)
    press_01 = result.loc[result["machine_id"] == "press_01"].iloc[0]

    assert press_01["telemetry_reading_count"] == 4
    assert press_01["fault_reading_count"] == 2
    assert press_01["fault_event_count"] == 1
    assert press_01["fault_reading_pct"] == 50.0
    assert press_01["dominant_fault_type"] == "OVERHEATING"
    assert press_01["distinct_fault_type_count"] == 1


def test_production_hourly_integrates_runtime_and_downtime():
    prepared = prepare_silver_telemetry(silver_fixture())
    result = build_production_hourly(
        prepared, max_observation_gap_seconds=60
    )
    press_01 = result.loc[result["machine_id"] == "press_01"].iloc[0]

    assert press_01["covered_seconds"] == 180.0
    assert press_01["estimated_runtime_seconds"] == 120.0
    assert press_01["estimated_downtime_seconds"] == 60.0
    assert press_01["availability_pct"] == 66.67
    assert press_01["estimated_revolutions"] == 6000.0


def test_missing_contract_column_fails_fast():
    broken = silver_fixture().drop(columns=["rpm"])

    with pytest.raises(ValueError, match="missing required columns.*rpm"):
        prepare_silver_telemetry(broken)


def test_non_positive_observation_gap_is_rejected():
    prepared = prepare_silver_telemetry(silver_fixture())

    with pytest.raises(ValueError, match="greater than zero"):
        build_production_hourly(prepared, max_observation_gap_seconds=0)


def test_run_pipeline_writes_reconciled_tables_and_manifest(tmp_path):
    silver_file = tmp_path / "silver.parquet"
    gold_folder = tmp_path / "gold"
    silver_fixture().to_parquet(silver_file, index=False)

    manifest = run_pipeline(
        silver_file=silver_file,
        gold_folder=gold_folder,
        max_observation_gap_seconds=60,
    )

    maintenance = pd.read_parquet(gold_folder / MAINTENANCE_FILE_NAME)
    production = pd.read_parquet(gold_folder / PRODUCTION_FILE_NAME)
    saved_manifest = json.loads(
        (gold_folder / MANIFEST_FILE_NAME).read_text(encoding="utf-8")
    )

    assert manifest["accepted_row_count"] == 5
    assert saved_manifest["quality_checks"]["status"] == "passed"
    assert maintenance["telemetry_reading_count"].sum() == 5
    assert production["telemetry_reading_count"].sum() == 5
