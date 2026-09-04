from fastapi import FastAPI

app = FastAPI(title="Industrial IoT Multi-Agent Platform")

@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
