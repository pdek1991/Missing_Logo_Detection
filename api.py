import sys
from pathlib import Path
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel


class StatusOut(BaseModel):
    channel: str
    score: Optional[float]
    status: str
    timestamp: str


class DashboardStatusOut(BaseModel):
    channel: str
    logo: Optional[str]
    status: str
    last_checked: str
    confidence: Optional[float]
    detection_confidence: Optional[float]
    last_detection_time: str
    raw_status: Optional[str]
    error: Optional[str]
    retries: int


def create_app(status_store, web_root="web", logos_root="logos"):
    app = FastAPI(title="Logo Detector API", version="2.0")

    web_path = _resolve_web_root(web_root)
    logos_path = _resolve_runtime_path(logos_root)

    assets_path = web_path / "assets"
    if assets_path.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_path)), name="assets")

    if logos_path.exists():
        app.mount("/logos", StaticFiles(directory=str(logos_path)), name="logos")

    @app.get("/health")
    def health_check():
        rows = status_store.all()
        return {
            "status": "ok",
            "channels": len(rows),
        }

    @app.get("/status", response_model=List[StatusOut])
    def get_all_status():
        return status_store.all()

    @app.get("/status/{channel}", response_model=StatusOut)
    def get_channel_status(channel: str):
        data = status_store.get(channel)
        if data is None:
            raise HTTPException(status_code=404, detail="Channel not found")
        return data

    @app.get("/api/logo_status", response_model=List[DashboardStatusOut])
    def get_logo_status():
        return status_store.dashboard_all()

    @app.get("/api/logo_status/{channel}", response_model=DashboardStatusOut)
    def get_logo_status_by_channel(channel: str):
        rows = status_store.dashboard_all()
        for row in rows:
            if row["channel"] == channel:
                return row
        raise HTTPException(status_code=404, detail="Channel not found")

    @app.get("/", include_in_schema=False)
    def index_page():
        index_path = web_path / "index.html"
        if not index_path.exists():
            raise HTTPException(status_code=404, detail="Dashboard not found")
        return FileResponse(index_path)

    @app.get("/styles.css", include_in_schema=False)
    def styles():
        return _static_file_or_404(web_path / "styles.css")

    @app.get("/app.js", include_in_schema=False)
    def app_js():
        return _static_file_or_404(web_path / "app.js")

    @app.get("/manifest.json", include_in_schema=False)
    def manifest():
        return _static_file_or_404(web_path / "manifest.json")

    @app.get("/service-worker.js", include_in_schema=False)
    def service_worker():
        return _static_file_or_404(web_path / "service-worker.js")

    @app.get("/favicon.ico", include_in_schema=False)
    def favicon():
        return _static_file_or_404(web_path / "assets" / "icons" / "logo.ico")

    return app


def _static_file_or_404(path: Path):
    if not path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(path)


def _resolve_runtime_path(relative_path: str) -> Path:
    local_path = Path(relative_path)
    if local_path.exists():
        return local_path

    meipass = getattr(sys, "_MEIPASS", None)
    if meipass:
        bundled_path = Path(meipass) / relative_path
        if bundled_path.exists():
            return bundled_path

    return local_path


def _resolve_web_root(web_root: str) -> Path:
    local_path = Path(web_root)
    meipass = getattr(sys, "_MEIPASS", None)
    bundled_path = (Path(meipass) / web_root) if meipass else None

    for candidate in [local_path, bundled_path]:
        if candidate and (candidate / "index.html").exists() and (candidate / "app.js").exists():
            return candidate

    return _resolve_runtime_path(web_root)
