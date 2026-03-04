import threading
from datetime import datetime, timezone

API_STATUS_PRESENT = "LOGO_PRESENT"
API_STATUS_MAYBE_MISSING = "LOGO_MAYBE_MISSING"
API_STATUS_MISSING = "LOGO_MISSING"

SEVERITY_GREEN = "GREEN"
SEVERITY_YELLOW = "YELLOW"
SEVERITY_RED = "RED"

SEVERITY_PRIORITY = {
    SEVERITY_RED: 0,
    SEVERITY_YELLOW: 1,
    SEVERITY_GREEN: 2,
}


def _normalize_status(raw_status):
    if raw_status is None:
        return ""
    normalized = str(raw_status).strip().upper().replace("_", " ")
    normalized = " ".join(normalized.split())
    return normalized


def map_status(raw_status):
    normalized = _normalize_status(raw_status)

    if normalized == "LOGO PRESENT":
        return API_STATUS_PRESENT

    if normalized in {
        "LOGO MAYBE MISSING",
        "WARMUP",
        "VERIFYING",
        "VERIFICATION TRIGGERED",
        "VERIFICATION FAILED",
    }:
        return API_STATUS_MAYBE_MISSING

    if normalized in {
        "LOGO MISSING",
        "NO PACKETS RECEIVED",
        "STREAM TIMEOUT",
        "STREAM CORRUPT",
        "STREAM DECODE ERROR",
        "STREAM DOWN",
        "STREAM UNAVAILABLE",
        "STREAM IO ERROR",
        "FROZEN FRAME",
    }:
        return API_STATUS_MISSING

    if normalized.startswith("STREAM"):
        return API_STATUS_MISSING

    return API_STATUS_MAYBE_MISSING


def map_severity(raw_status):
    api_status = map_status(raw_status)
    if api_status == API_STATUS_PRESENT:
        return SEVERITY_GREEN
    if api_status == API_STATUS_MAYBE_MISSING:
        return SEVERITY_YELLOW
    return SEVERITY_RED


class StatusStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._data = {}

    def register_stream(self, channel, logo):
        now = datetime.now(timezone.utc)
        payload = {
            "channel": channel,
            "score": None,
            "status": API_STATUS_MAYBE_MISSING,
            "severity": SEVERITY_YELLOW,
            "raw_status": "WARMUP",
            "timestamp": now.isoformat().replace("+00:00", "Z"),
            "last_checked": now.astimezone().strftime("%H:%M:%S"),
            "logo": logo,
            "error": None,
            "retries": 0,
            "consecutive_failures": 0,
            "stream_available": False,
        }
        with self._lock:
            self._data[channel] = payload

    def update(
        self,
        channel,
        score,
        raw_status,
        logo=None,
        error=None,
        retries=0,
        consecutive_failures=0,
    ):
        score_value = None
        if score not in (None, "N/A"):
            try:
                score_value = float(score)
            except (TypeError, ValueError):
                score_value = None

        now = datetime.now(timezone.utc)
        payload = {
            "channel": channel,
            "score": score_value,
            "status": map_status(raw_status),
            "severity": map_severity(raw_status),
            "raw_status": str(raw_status),
            "timestamp": now.isoformat().replace("+00:00", "Z"),
            "last_checked": now.astimezone().strftime("%H:%M:%S"),
            "logo": logo,
            "error": error,
            "retries": int(max(0, retries)),
            "consecutive_failures": int(max(0, consecutive_failures)),
            "stream_available": not bool(error),
        }

        with self._lock:
            existing = self._data.get(channel, {})
            if payload["logo"] is None:
                payload["logo"] = existing.get("logo")
            self._data[channel] = payload

    def get(self, channel):
        with self._lock:
            if channel not in self._data:
                return None
            return dict(self._data[channel])

    def all(self):
        with self._lock:
            return [dict(self._data[k]) for k in sorted(self._data.keys())]

    def dashboard_all(self):
        with self._lock:
            rows = [
                {
                    "channel": value["channel"],
                    "logo": value.get("logo"),
                    "status": value.get("severity", SEVERITY_YELLOW),
                    "last_checked": value.get("last_checked"),
                    "confidence": value.get("score"),
                    "detection_confidence": value.get("score"),
                    "last_detection_time": value.get("timestamp"),
                    "raw_status": value.get("raw_status"),
                    "error": value.get("error"),
                    "retries": value.get("retries", 0),
                }
                for value in self._data.values()
            ]

        rows.sort(key=lambda row: (SEVERITY_PRIORITY.get(row["status"], 99), row["channel"]))
        return rows
