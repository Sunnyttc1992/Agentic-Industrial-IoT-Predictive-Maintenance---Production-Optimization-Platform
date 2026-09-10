"""Latest validated machine telemetry routes."""

from fastapi import APIRouter, HTTPException

from ..services import repository


router = APIRouter(prefix="/api/v1/machines", tags=["machines"])


@router.get("")
def list_machines() -> dict[str, object]:
    """Return the latest validated reading for every known machine."""
    machines = repository.latest_telemetry()
    return {"count": len(machines), "machines": machines}


@router.get("/{machine_id}")
def get_machine(machine_id: str) -> dict[str, object]:
    """Return the latest validated reading for one machine."""
    machine = next(
        (
            item
            for item in repository.latest_telemetry()
            if item["machine_id"].lower() == machine_id.lower()
        ),
        None,
    )
    if machine is None:
        raise HTTPException(status_code=404, detail=f"Unknown machine: {machine_id}")
    return machine
