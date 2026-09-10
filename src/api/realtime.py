"""Live MQTT analytics and WebSocket delivery for the operations console.

The batch Silver/Gold pipeline remains the system of record for historical
analytics.  This module handles the low-latency path: MQTT telemetry is
validated, enriched with rolling features, scored, and delivered to dashboard
clients without writing to the local Parquet artifacts for every event.
"""

from __future__ import annotations

import asyncio
import json
import os
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any, Callable

import joblib
import numpy as np
import pandas as pd
from awscrt import mqtt
from awsiot import mqtt_connection_builder
from dotenv import load_dotenv
from fastapi import WebSocket
from pydantic import ValidationError

from src.agents.graph import run_supervisor
from src.streaming.stream_processor import process as process_stream_event

from .schemas import TelemetryEvent
from .services import approval_store, alert_store, repository


PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIRECTORY = PROJECT_ROOT / "models"
SENSOR_FIELDS = (
    "temperature_c",
    "vibration_mm_s",
    "current_amp",
    "pressure_bar",
    "rpm",
)
FEATURE_COLUMNS = tuple(
    feature
    for sensor in SENSOR_FIELDS
    for feature in (
        sensor,
        f"{sensor}_change",
        f"{sensor}_rolling_mean_5",
        f"{sensor}_rolling_std_5",
        f"{sensor}_deviation",
    )
)


class ConnectionManager:
    """Manage dashboard WebSocket clients within the FastAPI event loop."""

    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._connections.add(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        self._connections.discard(websocket)

    async def broadcast(self, message: dict[str, Any]) -> None:
        disconnected: list[WebSocket] = []
        for websocket in tuple(self._connections):
            try:
                await websocket.send_json(message)
            except (RuntimeError, OSError):
                disconnected.append(websocket)
        for websocket in disconnected:
            self.disconnect(websocket)


class RollingFeatureBuilder:
    """Build the same five-observation features used to train the ML models."""

    def __init__(self, window_size: int = 5) -> None:
        self._window_size = window_size
        self._history: dict[str, deque[dict[str, float]]] = defaultdict(
            lambda: deque(maxlen=window_size)
        )

    def build(self, event: dict[str, Any]) -> dict[str, float]:
        machine_id = str(event["machine_id"])
        current = {field: float(event[field]) for field in SENSOR_FIELDS}
        history = self._history[machine_id]
        observations = [*history, current]
        previous = history[-1] if history else current
        features: dict[str, float] = {}

        for sensor in SENSOR_FIELDS:
            values = np.asarray(
                [observation[sensor] for observation in observations], dtype=float
            )
            rolling_mean = float(values.mean())
            features[sensor] = current[sensor]
            features[f"{sensor}_change"] = current[sensor] - previous[sensor]
            features[f"{sensor}_rolling_mean_5"] = rolling_mean
            features[f"{sensor}_rolling_std_5"] = float(values.std(ddof=0))
            features[f"{sensor}_deviation"] = current[sensor] - rolling_mean

        history.append(current)
        return features


class OnlinePredictor:
    """Score a single feature row with the trained fault and anomaly models."""

    def __init__(self, model_directory: Path = MODEL_DIRECTORY) -> None:
        self._model_directory = model_directory
        self._loaded = False
        self._load_error: str | None = None
        self._fault_bundle: dict[str, Any] | None = None
        self._anomaly_model: Any | None = None
        self._scaler: Any | None = None

    def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        try:
            self._fault_bundle = joblib.load(
                self._model_directory / "fault_classifier.joblib"
            )
            self._anomaly_model = joblib.load(
                self._model_directory / "isolation_forest.joblib"
            )
            self._scaler = joblib.load(self._model_directory / "scaler.joblib")
        except (OSError, ValueError, TypeError, AttributeError) as error:
            self._load_error = str(error)

    def predict(self, event: dict[str, Any], features: dict[str, float]) -> dict[str, Any]:
        """Return a serializable prediction; preserve live telemetry if unavailable."""
        self._load()
        base = {
            "machine_id": event["machine_id"],
            "timestamp": event["timestamp"],
            "model_available": self._load_error is None,
        }
        if self._load_error or not self._fault_bundle or self._scaler is None:
            return {
                **base,
                "fault_probability": None,
                "predicted_fault": None,
                "predicted_fault_type": None,
                "alert_threshold": None,
                "anomaly_detected": None,
                "anomaly_score": None,
                "model_error": self._load_error or "Model artifacts are unavailable.",
            }

        try:
            feature_columns = self._fault_bundle["feature_columns"]
            feature_frame = pd.DataFrame(
                [[features[column] for column in feature_columns]],
                columns=feature_columns,
            )
            fault_model = self._fault_bundle["model"]
            probabilities = fault_model.predict_proba(feature_frame)[0]
            classes = np.asarray(fault_model.classes_)
            normal_label = self._fault_bundle["normal_label"]
            normal_index = int(np.where(classes == normal_label)[0][0])
            fault_probability = float(1 - probabilities[normal_index])
            threshold = float(self._fault_bundle["alert_threshold"])
            predicted_fault = int(fault_probability >= threshold)
            fault_indexes = np.where(classes != normal_label)[0]
            predicted_type = str(classes[fault_indexes[np.argmax(probabilities[fault_indexes])]])

            anomaly_detected: bool | None = None
            anomaly_score: float | None = None
            if self._anomaly_model is not None:
                scaled_features = self._scaler.transform(feature_frame)
                anomaly_detected = bool(self._anomaly_model.predict(scaled_features)[0] == -1)
                anomaly_score = float(self._anomaly_model.decision_function(scaled_features)[0])

            return {
                **base,
                "fault_probability": fault_probability,
                "predicted_fault": predicted_fault,
                "predicted_fault_type": predicted_type if predicted_fault else normal_label,
                "alert_threshold": threshold,
                "anomaly_detected": anomaly_detected,
                "anomaly_score": anomaly_score,
                "model_error": None,
            }
        except (KeyError, IndexError, TypeError, ValueError) as error:
            return {
                **base,
                "model_available": False,
                "fault_probability": None,
                "predicted_fault": None,
                "predicted_fault_type": None,
                "alert_threshold": None,
                "anomaly_detected": None,
                "anomaly_score": None,
                "model_error": str(error),
            }


class LiveAnalyticsPipeline:
    """Serialize live processing so rolling features stay ordered per message."""

    def __init__(
        self,
        broadcaster: ConnectionManager,
        predictor: OnlinePredictor | None = None,
        agent_runner: Callable[[dict[str, Any]], dict[str, Any]] = run_supervisor,
    ) -> None:
        self._broadcaster = broadcaster
        self._predictor = predictor or OnlinePredictor()
        self._agent_runner = agent_runner
        self._features = RollingFeatureBuilder()
        self._lock = asyncio.Lock()

    @staticmethod
    def _validate(event: dict[str, Any]) -> dict[str, Any]:
        telemetry = TelemetryEvent.model_validate(event).model_dump(mode="json")
        for field in ("fault_type", "is_fault"):
            if field in event:
                telemetry[field] = event[field]

        physical_limits = {
            "temperature_c": (-50, 200),
            "vibration_mm_s": (0, 100),
            "current_amp": (0, 500),
            "pressure_bar": (0, 100),
            "rpm": (0, 10_000),
        }
        invalid = [
            field
            for field, (minimum, maximum) in physical_limits.items()
            if not minimum <= float(telemetry[field]) <= maximum
        ]
        if invalid:
            raise ValueError(f"Out-of-range sensor values: {', '.join(invalid)}")
        return telemetry

    async def process_event(self, raw_event: dict[str, Any]) -> dict[str, Any] | None:
        """Process one MQTT payload and broadcast exactly one live update."""
        async with self._lock:
            try:
                telemetry = self._validate(raw_event)
            except (ValidationError, ValueError) as error:
                return {"accepted": False, "reason": str(error)}

            if not repository.accepts_live_telemetry(telemetry):
                return {"accepted": False, "reason": "duplicate_or_stale_event"}

            features = self._features.build(telemetry)
            prediction = await asyncio.to_thread(
                self._predictor.predict, telemetry, features
            )
            repository.record_live_analytics(telemetry, prediction)

            action_event = process_stream_event(telemetry)
            alert = None
            agent_review = None
            if action_event["review_required"]:
                alert_id = alert_store.alert_id(action_event)
                acknowledgement = alert_store.lookup(alert_id)
                alert = {
                    "alert_id": alert_id,
                    "machine_id": action_event["machine_id"],
                    "timestamp": action_event["timestamp"],
                    "event_type": action_event["event_type"],
                    "severity": action_event["severity"],
                    "triggered_rules": action_event["triggered_rules"],
                    "agent_targets": action_event["agent_targets"],
                    "review_required": True,
                    "status": "ACKNOWLEDGED" if acknowledgement else "OPEN",
                    **(acknowledgement or {}),
                }
                state = await asyncio.to_thread(self._agent_runner, telemetry)
                approval = approval_store.create_for_alert(state["action_event"])
                agent_review = {
                    "machine_id": telemetry["machine_id"],
                    "action_event": state["action_event"],
                    "selected_agents": state["selected_agents"],
                    "review_required": state["review_required"],
                    "approval": approval,
                }

            message = {
                "type": "realtime_update",
                "telemetry": telemetry,
                "prediction": prediction,
                "alert": alert,
                "agent_review": agent_review,
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
            await self._broadcaster.broadcast(message)
            return {"accepted": True, **message}


class AwsIotTelemetrySubscriber:
    """Own an AWS IoT Core MQTT subscription without blocking the API startup."""

    def __init__(self, pipeline: LiveAnalyticsPipeline) -> None:
        self._pipeline = pipeline
        self._connection: Any | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: Thread | None = None
        self._stop_event = Event()
        self._state_lock = Lock()
        self._status: dict[str, Any] = {
            "enabled": False,
            "connection_status": "NOT_STARTED",
            "topic": None,
            "received_messages": 0,
            "processed_messages": 0,
            "rejected_messages": 0,
            "last_message_at": None,
            "last_error": None,
        }

    @staticmethod
    def _enabled() -> bool:
        return os.getenv("REALTIME_MQTT_ENABLED", "false").strip().lower() in {
            "1",
            "true",
            "yes",
        }

    def status(self) -> dict[str, Any]:
        with self._state_lock:
            return dict(self._status)

    def _update_status(self, **updates: Any) -> None:
        with self._state_lock:
            self._status.update(updates)

    def start(self, loop: asyncio.AbstractEventLoop) -> None:
        """Start connecting in a daemon thread; API availability never depends on AWS."""
        load_dotenv(PROJECT_ROOT / ".env")
        self._loop = loop
        if not self._enabled():
            self._update_status(enabled=False, connection_status="DISABLED")
            return
        if self._thread and self._thread.is_alive():
            return

        self._stop_event.clear()
        self._update_status(enabled=True, connection_status="CONNECTING", last_error=None)
        self._thread = Thread(target=self._run, name="aws-iot-operations-feed", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._connection is not None:
            try:
                self._connection.disconnect().result()
            except Exception:  # Connection may already be closed during shutdown.
                pass
        self._update_status(connection_status="STOPPED")

    def _settings(self) -> dict[str, str]:
        settings = {
            "endpoint": os.getenv("AWS_IOT_ENDPOINT", ""),
            "cert_path": os.getenv("AWS_IOT_CERT", ""),
            "private_key_path": os.getenv("AWS_IOT_PRIVATE_KEY", ""),
            "root_ca_path": os.getenv("AWS_IOT_ROOT_CA", ""),
            "client_id": os.getenv("AWS_IOT_CLIENT_ID", ""),
            "topic": os.getenv(
                "REALTIME_MQTT_TOPIC", "sunny/factory1/machines/+/telemetry"
            ),
        }
        missing = [name for name, value in settings.items() if name != "topic" and not value]
        if missing:
            raise ValueError(f"Missing MQTT settings: {', '.join(missing)}")
        return settings

    def _run(self) -> None:
        try:
            settings = self._settings()
            dashboard_client_id = os.getenv(
                "AWS_IOT_DASHBOARD_CLIENT_ID",
                f"{settings['client_id']}-operations-console",
            )
            self._connection = mqtt_connection_builder.mtls_from_path(
                endpoint=settings["endpoint"],
                cert_filepath=settings["cert_path"],
                pri_key_filepath=settings["private_key_path"],
                ca_filepath=settings["root_ca_path"],
                client_id=dashboard_client_id,
                clean_session=False,
                keep_alive_secs=30,
                on_connection_interrupted=self._on_connection_interrupted,
                on_connection_resumed=self._on_connection_resumed,
            )
            self._connection.connect().result()
            self._connection.subscribe(
                settings["topic"], mqtt.QoS.AT_LEAST_ONCE, self._on_message
            )[0].result()
            self._update_status(connection_status="CONNECTED", topic=settings["topic"])
            self._stop_event.wait()
        except Exception as error:
            self._update_status(connection_status="ERROR", last_error=str(error))

    def _on_connection_interrupted(self, connection: Any, error: Exception, **_: Any) -> None:
        self._update_status(connection_status="RECONNECTING", last_error=str(error))

    def _on_connection_resumed(
        self, connection: Any, return_code: Any, session_present: bool, **_: Any
    ) -> None:
        if not session_present:
            try:
                connection.resubscribe_existing_topics()[0].result()
            except Exception as error:
                self._update_status(connection_status="ERROR", last_error=str(error))
                return
        self._update_status(connection_status="CONNECTED", last_error=None)

    def _on_message(self, topic: str, payload: bytes, **_: Any) -> None:
        self._update_status(
            received_messages=self.status()["received_messages"] + 1,
            last_message_at=datetime.now(timezone.utc).isoformat(),
        )
        try:
            event = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            self._update_status(
                rejected_messages=self.status()["rejected_messages"] + 1,
                last_error=f"Invalid MQTT payload: {error}",
            )
            return
        if self._loop is None or self._stop_event.is_set():
            return

        future = asyncio.run_coroutine_threadsafe(
            self._pipeline.process_event(event), self._loop
        )
        future.add_done_callback(self._record_result)

    def _record_result(self, future: Any) -> None:
        try:
            result = future.result()
            if result and result.get("accepted"):
                self._update_status(
                    processed_messages=self.status()["processed_messages"] + 1,
                    last_error=None,
                )
            else:
                self._update_status(
                    rejected_messages=self.status()["rejected_messages"] + 1,
                    last_error=(result or {}).get("reason", "Event processing failed."),
                )
        except Exception as error:
            self._update_status(
                rejected_messages=self.status()["rejected_messages"] + 1,
                last_error=str(error),
            )


connection_manager = ConnectionManager()
live_pipeline = LiveAnalyticsPipeline(connection_manager)
mqtt_subscriber = AwsIotTelemetrySubscriber(live_pipeline)
