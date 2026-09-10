"""Local data access and in-memory review stores for the application layer."""

from __future__ import annotations

from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from threading import Lock
from typing import Any
from uuid import uuid4

import pandas as pd

from src.streaming.stream_processor import process as process_stream_event


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_FILE = PROJECT_ROOT / "data" / "silver" / "telemetry_clean.parquet"
FAULT_PREDICTIONS_FILE = (
    PROJECT_ROOT / "data" / "model_output" / "fault_predictions.parquet"
)
ANOMALY_PREDICTIONS_FILE = (
    PROJECT_ROOT / "data" / "model_output" / "anomaly_predictions.parquet"
)


def _timestamp_value(value: Any) -> str | None:
    if pd.isna(value):
        return None
    return pd.Timestamp(value).isoformat()


def _number_value(value: Any) -> int | float | None:
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    number = float(value)
    return int(number) if number.is_integer() else number


class LocalDataRepository:
    """Serve historical artifacts with a live, in-memory telemetry overlay."""

    def __init__(self) -> None:
        self._live_telemetry: dict[str, dict[str, Any]] = {}
        self._live_predictions: dict[str, dict[str, Any]] = {}
        self._live_lock = Lock()

    @staticmethod
    def _read_parquet(path: Path) -> pd.DataFrame:
        return pd.read_parquet(path) if path.is_file() else pd.DataFrame()

    def latest_telemetry(self) -> list[dict[str, Any]]:
        telemetry = self._read_parquet(SILVER_FILE)
        required_columns = {
            "machine_id",
            "timestamp",
            "temperature_c",
            "vibration_mm_s",
            "current_amp",
            "pressure_bar",
            "rpm",
            "status",
        }
        historical: list[dict[str, Any]] = []
        if not telemetry.empty and required_columns.issubset(telemetry.columns):
            telemetry = telemetry.copy()
            telemetry["timestamp"] = pd.to_datetime(
                telemetry["timestamp"], utc=True, errors="coerce"
            )
            if "data_quality_pass" in telemetry.columns:
                telemetry = telemetry.loc[
                    telemetry["data_quality_pass"].fillna(False).astype(bool)
                ]
            telemetry = telemetry.dropna(subset=list(required_columns))
            latest = (
                telemetry.sort_values("timestamp")
                .groupby("machine_id", as_index=False)
                .tail(1)
                .sort_values("machine_id")
            )
            historical = [
            {
                "machine_id": str(row.machine_id),
                "timestamp": _timestamp_value(row.timestamp),
                "temperature_c": _number_value(row.temperature_c),
                "vibration_mm_s": _number_value(row.vibration_mm_s),
                "current_amp": _number_value(row.current_amp),
                "pressure_bar": _number_value(row.pressure_bar),
                "rpm": _number_value(row.rpm),
                "status": str(row.status),
                "fault_type": (
                    None
                    if "fault_type" not in latest.columns or pd.isna(row.fault_type)
                    else str(row.fault_type)
                ),
                "is_fault": (
                    None
                    if "is_fault" not in latest.columns or pd.isna(row.is_fault)
                    else bool(row.is_fault)
                ),
            }
                for row in latest.itertuples(index=False)
            ]
        with self._live_lock:
            records = {record["machine_id"]: record for record in historical}
            records.update(self._live_telemetry)
        return sorted(records.values(), key=lambda record: record["machine_id"])

    def latest_fault_predictions(self) -> list[dict[str, Any]]:
        predictions = self._read_parquet(FAULT_PREDICTIONS_FILE)
        historical: list[dict[str, Any]] = []
        fields = [
            "fault_type",
            "fault_probability",
            "predicted_fault",
            "predicted_fault_type",
            "alert_threshold",
        ]
        if not predictions.empty and {"machine_id", "timestamp"}.issubset(
            predictions.columns
        ):
            predictions = predictions.copy()
            predictions["timestamp"] = pd.to_datetime(
                predictions["timestamp"], utc=True, errors="coerce"
            )
            latest = (
                predictions.dropna(subset=["timestamp"])
                .sort_values("timestamp")
                .groupby("machine_id", as_index=False)
                .tail(1)
                .sort_values("machine_id")
            )
            historical = [
            {
                "machine_id": str(row.machine_id),
                "timestamp": _timestamp_value(row.timestamp),
                **{
                    field: (
                        None
                        if field not in latest.columns or pd.isna(getattr(row, field))
                        else _number_value(getattr(row, field))
                        if field
                        in {"fault_probability", "predicted_fault", "alert_threshold"}
                        else str(getattr(row, field))
                    )
                    for field in fields
                },
            }
                for row in latest.itertuples(index=False)
            ]
        with self._live_lock:
            records = {record["machine_id"]: record for record in historical}
            records.update(self._live_predictions)
        return sorted(records.values(), key=lambda record: record["machine_id"])

    def accepts_live_telemetry(self, telemetry: dict[str, Any]) -> bool:
        """Reject duplicate and late telemetry before it changes rolling state."""
        with self._live_lock:
            existing = self._live_telemetry.get(telemetry["machine_id"])
            return existing is None or telemetry["timestamp"] > existing["timestamp"]

    def record_live_analytics(
        self, telemetry: dict[str, Any], prediction: dict[str, Any]
    ) -> None:
        """Upsert the latest accepted telemetry and model result for one asset."""
        record = {
            key: telemetry.get(key)
            for key in (
                "machine_id",
                "timestamp",
                "temperature_c",
                "vibration_mm_s",
                "current_amp",
                "pressure_bar",
                "rpm",
                "status",
                "fault_type",
                "is_fault",
            )
        }
        with self._live_lock:
            self._live_telemetry[telemetry["machine_id"]] = record
            self._live_predictions[telemetry["machine_id"]] = prediction

    def data_status(self) -> dict[str, bool]:
        with self._live_lock:
            live_telemetry_available = bool(self._live_telemetry)
        return {
            "silver_telemetry_available": SILVER_FILE.is_file(),
            "fault_predictions_available": FAULT_PREDICTIONS_FILE.is_file(),
            "anomaly_predictions_available": ANOMALY_PREDICTIONS_FILE.is_file(),
            "live_telemetry_available": live_telemetry_available,
        }


class AlertStore:
    """Store local acknowledgement metadata against deterministic alert IDs."""

    def __init__(self) -> None:
        self._acknowledgements: dict[str, dict[str, Any]] = {}
        self._lock = Lock()

    @staticmethod
    def alert_id(action_event: dict[str, Any]) -> str:
        identity = "|".join(
            [
                action_event["machine_id"],
                action_event["timestamp"],
                action_event["event_type"],
            ]
        )
        return sha256(identity.encode("utf-8")).hexdigest()[:16]

    def acknowledge(
        self, alert_id: str, acknowledged_by: str, note: str | None
    ) -> dict[str, Any]:
        acknowledgement = {
            "acknowledged_by": acknowledged_by,
            "acknowledged_at": datetime.now(timezone.utc).isoformat(),
            "acknowledgement_note": note,
        }
        with self._lock:
            self._acknowledgements[alert_id] = acknowledgement
        return acknowledgement

    def lookup(self, alert_id: str) -> dict[str, Any] | None:
        with self._lock:
            return self._acknowledgements.get(alert_id)


class ApprovalStore:
    """In-memory review state for local development and demonstrations only."""

    def __init__(self) -> None:
        self._approvals: dict[str, dict[str, Any]] = {}
        self._approval_by_alert_id: dict[str, str] = {}
        self._lock = Lock()

    def create(self, action_event: dict[str, Any]) -> dict[str, Any]:
        approval = {
            "approval_id": uuid4().hex,
            "machine_id": action_event["machine_id"],
            "event_type": action_event["event_type"],
            "severity": action_event["severity"],
            "status": "PENDING",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "decision": None,
            "reviewer": None,
            "notes": None,
            "decided_at": None,
        }
        with self._lock:
            self._approvals[approval["approval_id"]] = approval
        return approval

    def create_for_alert(self, action_event: dict[str, Any]) -> dict[str, Any]:
        """Create at most one pending review for a uniquely identified live alert."""
        alert_id = alert_store.alert_id(action_event)
        with self._lock:
            existing_id = self._approval_by_alert_id.get(alert_id)
            if existing_id:
                return self._approvals[existing_id]
            approval = {
                "approval_id": uuid4().hex,
                "machine_id": action_event["machine_id"],
                "event_type": action_event["event_type"],
                "severity": action_event["severity"],
                "status": "PENDING",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "decision": None,
                "reviewer": None,
                "notes": None,
                "decided_at": None,
            }
            self._approvals[approval["approval_id"]] = approval
            self._approval_by_alert_id[alert_id] = approval["approval_id"]
            return approval

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            return sorted(
                self._approvals.values(),
                key=lambda approval: approval["created_at"],
                reverse=True,
            )

    def decide(
        self, approval_id: str, decision: str, reviewer: str, notes: str | None
    ) -> dict[str, Any] | None:
        with self._lock:
            approval = self._approvals.get(approval_id)
            if approval is None:
                return None
            if approval["status"] != "PENDING":
                return approval

            approval.update(
                {
                    "status": decision,
                    "decision": decision,
                    "reviewer": reviewer,
                    "notes": notes,
                    "decided_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            return approval


repository = LocalDataRepository()
alert_store = AlertStore()
approval_store = ApprovalStore()


def current_alerts() -> list[dict[str, Any]]:
    """Create actionable alerts from each machine's latest validated telemetry."""
    alerts: list[dict[str, Any]] = []
    for telemetry in repository.latest_telemetry():
        action_event = process_stream_event(telemetry)
        if not action_event["review_required"]:
            continue

        alert_id = alert_store.alert_id(action_event)
        acknowledgement = alert_store.lookup(alert_id)
        alerts.append(
            {
                "alert_id": alert_id,
                "machine_id": action_event["machine_id"],
                "timestamp": action_event["timestamp"],
                "event_type": action_event["event_type"],
                "severity": action_event["severity"],
                "triggered_rules": action_event["triggered_rules"],
                "agent_targets": action_event["agent_targets"],
                "review_required": action_event["review_required"],
                "status": "ACKNOWLEDGED" if acknowledgement else "OPEN",
                **(acknowledgement or {}),
            }
        )

    return sorted(alerts, key=lambda alert: (alert["severity"], alert["timestamp"]))
