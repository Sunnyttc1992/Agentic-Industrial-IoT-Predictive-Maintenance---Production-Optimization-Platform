"""Dashboard asset location used by the FastAPI application layer."""

from pathlib import Path


DASHBOARD_DIRECTORY = Path(__file__).resolve().parent
DASHBOARD_INDEX = DASHBOARD_DIRECTORY / "index.html"
