"""Latest model prediction routes."""

from fastapi import APIRouter, HTTPException

from ..services import repository


router = APIRouter(prefix="/api/v1/predictions", tags=["predictions"])


@router.get("")
def list_predictions() -> dict[str, object]:
    """Return each machine's most recent supervised fault prediction."""
    predictions = repository.latest_fault_predictions()
    return {"count": len(predictions), "predictions": predictions}


@router.get("/{machine_id}")
def get_prediction(machine_id: str) -> dict[str, object]:
    """Return the most recent supervised fault prediction for one machine."""
    prediction = next(
        (
            item
            for item in repository.latest_fault_predictions()
            if item["machine_id"].lower() == machine_id.lower()
        ),
        None,
    )
    if prediction is None:
        raise HTTPException(
            status_code=404, detail=f"No prediction found for machine: {machine_id}"
        )
    return prediction
