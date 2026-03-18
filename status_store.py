import threading
from collections import deque
from datetime import datetime, timezone

API_STATUS_PRESENT = "LOGO_PRESENT"
API_STATUS_MAYBE_MISSING = "LOGO_MAYBE_MISSING"
API_STATUS_MISSING = "LOGO_MISSING"

SEVERITY_GREEN = "GREEN"
SEVERITY_WHITE = "WHITE"
SEVERITY_YELLOW = "YELLOW"
SEVERITY_RED = "RED"


def _normalize_status(raw_status):
    if raw_status is None:
        return ""
    normalized = str(raw_status).strip().upper().replace("_", " ")
    normalized = " ".join(normalized.split())
    return normalized


def _is_white_international_status(normalized):
    return normalized == "LOGO OFF SCHEDULED" or "INTERNATIONAL BREAK" in normalized or normalized in {
        "INTENTIONAL BREAK",
        "INTENTIONAL LOGO MISSING",
        "INTERNTIONAL LOGO MISSING",
        "INTERNATIONAL LOGO MISSING",
    }


def map_status(raw_status):
    normalized = _normalize_status(raw_status)

    if normalized == "LOGO PRESENT" or _is_white_international_status(normalized):
        return API_STATUS_PRESENT

    if normalized in {
        "MISSING DETECTED",
        "MISSING TEMPLATE",
    }:
        return API_STATUS_MISSING

    return API_STATUS_MAYBE_MISSING


def map_severity(raw_status):
    normalized = _normalize_status(raw_status)
    if _is_white_international_status(normalized):
        return SEVERITY_WHITE

    if (
        normalized.startswith("STREAM ")
        or normalized in {"NO PACKETS RECEIVED", "FROZEN FRAME", "STREAM DOWN"}
    ):
        return SEVERITY_RED

    if normalized == "LOGO PRESENT":
        return SEVERITY_GREEN

    if normalized in {
        "MISSING DETECTED",
        "MISSING TEMPLATE",
    }:
        return SEVERITY_RED

    return SEVERITY_YELLOW


class StatusStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._data = {}
        self._history = {}
        self._version = 0
        self._channel_order = []

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
        with self._condition:
            self._data[channel] = payload
            if channel not in self._channel_order:
                self._channel_order.append(channel)

            history = self._history.setdefault(channel, deque(maxlen=300))
            if not history:
                history.append(payload["severity"])

            self._version += 1
            self._condition.notify_all()

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

        with self._condition:
            existing = self._data.get(channel, {})
            if payload["logo"] is None:
                payload["logo"] = existing.get("logo")

            changed = self._has_significant_change(existing, payload)
            if not changed:
                return

            self._data[channel] = payload
            if channel not in self._channel_order:
                self._channel_order.append(channel)

            history = self._history.setdefault(channel, deque(maxlen=300))
            if not history or history[-1] != payload["severity"]:
                history.append(payload["severity"])
            self._version += 1
            self._condition.notify_all()

    def get(self, channel):
        with self._lock:
            if channel not in self._data:
                return None
            return dict(self._data[channel])

    def all(self):
        with self._lock:
            rows = []
            for channel in self._ordered_channels():
                if channel in self._data:
                    rows.append(dict(self._data[channel]))
            return rows

    def dashboard_all(self):
        with self._lock:
            return self._build_dashboard_rows()

    def dashboard_get(self, channel):
        with self._lock:
            value = self._data.get(channel)
            if value is None:
                return None
            return self._dashboard_row(value)

    def wait_for_updates(self, last_version, timeout=10.0):
        with self._condition:
            if self._version <= int(last_version):
                self._condition.wait(timeout=max(0.1, float(timeout)))

            if self._version <= int(last_version):
                return self._version, None

            payload = self._build_dashboard_rows()
            return self._version, payload

    def _build_dashboard_rows(self):
        rows = []
        for channel in self._ordered_channels():
            value = self._data.get(channel)
            if value is None:
                continue
            rows.append(self._dashboard_row(value))
        return rows

    def _ordered_channels(self):
        if not self._data:
            return []

        ordered = [channel for channel in self._channel_order if channel in self._data]

        ordered_set = set(ordered)
        extras = [channel for channel in self._data.keys() if channel not in ordered_set]
        extras.sort()
        if extras:
            ordered.extend(extras)

        return ordered

    def _dashboard_row(self, value):
        channel = value["channel"]
        history = self._history.get(channel)
        return {
            "channel": channel,
            "logo": value.get("logo"),
            "status": value.get("severity", SEVERITY_YELLOW),
            "last_checked": value.get("last_checked"),
            "confidence": value.get("score"),
            "detection_confidence": value.get("score"),
            "last_detection_time": value.get("timestamp"),
            "raw_status": value.get("raw_status"),
            "error": value.get("error"),
            "retries": value.get("retries", 0),
            "timeline": self._build_timeline(history),
            "stability_percent": self._stability_percent(history),
        }

    @staticmethod
    def _has_significant_change(existing, payload):
        if not existing:
            return True

        old_score = existing.get("score")
        new_score = payload.get("score")
        if old_score is None and new_score is not None:
            return True
        if old_score is not None and new_score is None:
            return True
        if old_score is not None and new_score is not None:
            if abs(float(old_score) - float(new_score)) >= 0.001:
                return True

        keys = (
            "status",
            "severity",
            "raw_status",
            "error",
            "logo",
            "stream_available",
        )
        for key in keys:
            if existing.get(key) != payload.get(key):
                return True

        return False

    @staticmethod
    def _build_timeline(history, points=24):
        data = list(history or [])
        if not data:
            return []

        if len(data) <= points:
            return data

        step = len(data) / float(points)
        sampled = []
        for idx in range(points):
            sample_index = min(len(data) - 1, int(round(idx * step)))
            sampled.append(data[sample_index])
        return sampled

    @staticmethod
    def _stability_percent(history):
        data = list(history or [])
        if not data:
            return 100.0

        stable = sum(1 for value in data if value in {SEVERITY_GREEN, SEVERITY_WHITE})
        return round((stable / float(len(data))) * 100.0, 1)
