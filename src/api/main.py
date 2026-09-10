"""Operator-facing API, alerts, approvals, dashboard, and live telemetry."""

import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from src.dashboard.app import DASHBOARD_DIRECTORY, DASHBOARD_INDEX

from .routes.agents import router as agents_router
from .routes.alerts import router as alerts_router
from .routes.machines import router as machines_router
from .routes.predictions import router as predictions_router
from .services import approval_store, current_alerts, repository
from .realtime import connection_manager, mqtt_subscriber


@asynccontextmanager
async def lifespan(_: FastAPI):
    """Run the optional AWS IoT subscriber without blocking the HTTP service."""
    mqtt_subscriber.start(asyncio.get_running_loop())
    try:
        yield
    finally:
        mqtt_subscriber.stop()


app = FastAPI(
    title="Industrial IoT Operations Console",
    version="0.7.0",
    description=(
        "Local application layer for telemetry, predictions, alerts, and "
        "human-reviewed agent recommendations."
    ),
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:8000", "http://localhost:8000"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)
app.include_router(machines_router)
app.include_router(predictions_router)
app.include_router(alerts_router)
app.include_router(agents_router)
app.mount("/static", StaticFiles(directory=DASHBOARD_DIRECTORY), name="static")


@app.get("/", include_in_schema=False)
def dashboard() -> FileResponse:
    """Serve the dependency-free local operations dashboard."""
    return FileResponse(DASHBOARD_INDEX)


@app.get("/health")
def health() -> dict[str, object]:
    """Report service, historical-artifact, and real-time connection health."""
    return {
        "status": "ok",
        "data": repository.data_status(),
        "realtime": mqtt_subscriber.status(),
    }


@app.websocket("/ws/operations")
async def operations_feed(websocket: WebSocket) -> None:
    """Push live telemetry analytics to one dashboard connection."""
    await connection_manager.connect(websocket)
    await websocket.send_json(
        {
            "type": "snapshot",
            "machines": repository.latest_telemetry(),
            "predictions": repository.latest_fault_predictions(),
            "alerts": current_alerts(),
            "approvals": approval_store.list(),
            "realtime": mqtt_subscriber.status(),
        }
    )
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        connection_manager.disconnect(websocket)
