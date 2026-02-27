"""DQ Agent Web — FastAPI backend serving on port 8001."""

import logging
import os
import sys
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# Ensure backend modules are importable
sys.path.insert(0, os.path.dirname(__file__))

from routers import data, validation  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

ROOT_PATH = os.environ.get("ROOT_PATH", "/proxy/8001")

app = FastAPI(title="AI DQ Agent Web", root_path=ROOT_PATH)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(data.router, prefix="/api")
app.include_router(validation.router, prefix="/api")


@app.get("/api/health")
async def health():
    return {"status": "ok"}


# Serve frontend static files
frontend_dist = Path(__file__).parent.parent / "frontend" / "dist"

if frontend_dist.exists():
    # Mount entire dist as static files with html=True for SPA fallback
    app.mount("/", StaticFiles(directory=str(frontend_dist), html=True), name="frontend")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
