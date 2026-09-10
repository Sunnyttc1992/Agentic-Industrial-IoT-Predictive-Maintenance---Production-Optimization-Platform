"""Request and response models for the operator-facing API."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, Field


class TelemetryEvent(BaseModel):
    """Telemetry required to evaluate a live operational event."""

    machine_id: str = Field(min_length=1)
    timestamp: datetime
    temperature_c: float
    vibration_mm_s: float
    current_amp: float
    pressure_bar: float
    rpm: int
    status: str = "RUNNING"


class ApprovalDecision(str, Enum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"


class ApprovalDecisionRequest(BaseModel):
    """A human decision on a recommendation; it never actuates equipment."""

    decision: ApprovalDecision
    reviewer: str = Field(min_length=1, max_length=120)
    notes: str | None = Field(default=None, max_length=1_000)


class AlertAcknowledgementRequest(BaseModel):
    """Operator acknowledgement metadata for a surfaced alert."""

    acknowledged_by: str = Field(min_length=1, max_length=120)
    note: str | None = Field(default=None, max_length=1_000)
