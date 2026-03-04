import math
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from functools import partial
from pathlib import Path

import cv2
import uvicorn
import yaml
from loguru import logger

from api import create_app
from detector import LogoDetector
from ffmpeg_reader import FFmpegReader
from logger import log_status, setup_logger
from state_machine import StateMachine
from status_store import StatusStore
from utils import compute_perceptual_hash

DEFAULT_MAX_STREAMS = 50
DEFAULT_POLL_INTERVAL_SECONDS = 10.0


@dataclass
class StreamRuntimeState:
    name: str
    config: dict
    reader: FFmpegReader
    detector: LogoDetector
    state_machine: StateMachine
    poll_interval: float
    ffmpeg_timeout: float
    retry_attempts: int
    next_run_monotonic: float
    logo_url: str
    lock: threading.Lock = field(default_factory=threading.Lock)
    in_flight: bool = False
    consecutive_failures: int = 0
    roi_counter: int = 0
    roi_max_frames: int = 30


class Scheduler:
    def __init__(self, config_file):
        with open(config_file, "r", encoding="utf-8") as handle:
            self.config = yaml.safe_load(handle) or {}

        self.streams = self.config.get("streams") or []
        self.num_streams = len(self.streams)

        self._validate_config()

        scheduler_cfg = self.config.get("scheduler", {})
        self.max_streams = int(scheduler_cfg.get("max_streams", DEFAULT_MAX_STREAMS))
        self.default_poll_interval = float(
            scheduler_cfg.get("default_poll_interval_seconds", DEFAULT_POLL_INTERVAL_SECONDS)
        )
        self.default_ffmpeg_timeout = float(scheduler_cfg.get("default_ffmpeg_timeout_seconds", 4.0))
        self.default_retry_attempts = int(scheduler_cfg.get("default_retry_attempts", 1))
        self.retry_backoff_seconds = float(scheduler_cfg.get("retry_backoff_seconds", 1.0))
        self.retry_backoff_multiplier = float(scheduler_cfg.get("retry_backoff_multiplier", 2.0))
        self.retry_max_backoff_seconds = float(scheduler_cfg.get("retry_max_backoff_seconds", 4.0))
        self.dispatch_sleep_seconds = float(scheduler_cfg.get("dispatch_sleep_seconds", 0.1))

        if self.num_streams > self.max_streams:
            raise ValueError(f"Configured streams ({self.num_streams}) exceed max_streams ({self.max_streams}).")

        self.recommended_workers = self._calculate_recommended_workers()
        configured_workers = scheduler_cfg.get("max_workers")
        if configured_workers is None:
            self.max_workers = min(self.max_streams, max(1, self.recommended_workers))
        else:
            self.max_workers = min(self.max_streams, max(1, int(configured_workers)))

        self.max_workers = max(1, min(self.max_workers, self.num_streams))

        if self.max_workers < self.recommended_workers:
            logger.warning(
                "Configured max_workers={} is below recommended {} for {} streams @ {}s poll.",
                self.max_workers,
                self.recommended_workers,
                self.num_streams,
                self.default_poll_interval,
            )

        self.api_enabled = bool(self.config.get("api_enabled", True))
        self.api_host = str(self.config.get("api_host", "0.0.0.0"))
        self.api_port = int(self.config.get("api_port", 8000))

        self.running = True
        self.status_store = StatusStore()
        self.pool = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="stream-worker")
        self.stream_state = {}

        self._api_server = None
        self._api_thread = None

        self._initialize_stream_states()

    def _validate_config(self):
        if not self.streams:
            raise ValueError("No streams found in config.")

        if len(self.streams) > DEFAULT_MAX_STREAMS:
            raise ValueError(f"Maximum supported streams is {DEFAULT_MAX_STREAMS}.")

        required_fields = ["name", "url", "template_path"]
        names = set()
        for idx, stream in enumerate(self.streams):
            for field_name in required_fields:
                if field_name not in stream:
                    raise ValueError(f"Stream index {idx} missing field '{field_name}'.")

            name = str(stream["name"]).strip()
            if not name:
                raise ValueError(f"Stream index {idx} has an empty name.")
            if name in names:
                raise ValueError(f"Duplicate stream name detected: {name}")
            names.add(name)

            threshold_yellow = float(stream.get("threshold_yellow", 0.60))
            threshold_red = float(stream.get("threshold_red", 0.50))
            if threshold_red > threshold_yellow:
                raise ValueError(
                    f"Invalid thresholds for '{name}'. threshold_red ({threshold_red}) cannot exceed threshold_yellow ({threshold_yellow})."
                )

            stream.setdefault("threshold_yellow", threshold_yellow)
            stream.setdefault("threshold_red", threshold_red)

    def _initialize_stream_states(self):
        base_time = time.monotonic()
        stagger_gap = max(0.02, self.default_poll_interval / max(1, self.num_streams))

        for index, stream_cfg in enumerate(self.streams):
            name = str(stream_cfg["name"])
            poll_interval = float(
                stream_cfg.get(
                    "poll_interval_seconds",
                    stream_cfg.get("scan_cycle_seconds", self.default_poll_interval),
                )
            )
            if poll_interval <= 0:
                raise ValueError(f"Invalid poll interval for stream '{name}'.")

            ffmpeg_timeout = float(stream_cfg.get("ffmpeg_timeout_seconds", self.default_ffmpeg_timeout))
            retry_attempts = int(stream_cfg.get("retry_attempts", self.default_retry_attempts))
            retry_attempts = max(0, retry_attempts)

            reader = FFmpegReader(stream_cfg["url"], timeout_seconds=ffmpeg_timeout)
            detector = LogoDetector(stream_cfg["template_path"], stream_cfg.get("roi"))
            state_machine = StateMachine(stream_cfg)

            logo_url = self._resolve_logo_url(stream_cfg)
            next_run = base_time + (index * stagger_gap)

            runtime_state = StreamRuntimeState(
                name=name,
                config=stream_cfg,
                reader=reader,
                detector=detector,
                state_machine=state_machine,
                poll_interval=poll_interval,
                ffmpeg_timeout=ffmpeg_timeout,
                retry_attempts=retry_attempts,
                next_run_monotonic=next_run,
                logo_url=logo_url,
                roi_max_frames=max(1, int(stream_cfg.get("roi_max_frames", 30))),
            )

            self.stream_state[name] = runtime_state
            self.status_store.register_stream(name, logo_url)

    def _resolve_logo_url(self, stream_cfg):
        display_logo = stream_cfg.get("display_logo")
        template_path = stream_cfg.get("template_path", "")
        logo_source = display_logo or template_path

        normalized = str(logo_source).replace("\\", "/")
        if normalized.startswith("/logos/"):
            return normalized

        if normalized.startswith("logos/"):
            return f"/{normalized}"

        filename = os.path.basename(normalized)
        if filename:
            return f"/logos/{filename}"
        return None

    def _calculate_recommended_workers(self):
        worst_case_parallelism = 0.0
        scheduler_cfg = self.config.get("scheduler", {})
        default_poll = float(scheduler_cfg.get("default_poll_interval_seconds", DEFAULT_POLL_INTERVAL_SECONDS))
        default_timeout = float(scheduler_cfg.get("default_ffmpeg_timeout_seconds", 4.0))
        default_retries = int(scheduler_cfg.get("default_retry_attempts", 1))
        backoff = float(scheduler_cfg.get("retry_backoff_seconds", 1.0))
        backoff_multiplier = float(scheduler_cfg.get("retry_backoff_multiplier", 2.0))
        max_backoff = float(scheduler_cfg.get("retry_max_backoff_seconds", 4.0))

        for stream_cfg in self.streams:
            poll_interval = float(
                stream_cfg.get(
                    "poll_interval_seconds",
                    stream_cfg.get("scan_cycle_seconds", default_poll),
                )
            )
            timeout = float(stream_cfg.get("ffmpeg_timeout_seconds", default_timeout))
            retries = max(0, int(stream_cfg.get("retry_attempts", default_retries)))

            capture_budget = 0.0
            for attempt in range(retries + 1):
                capture_budget += max(1.0, timeout * (0.75 ** attempt))
                if attempt < retries:
                    capture_budget += min(backoff * (backoff_multiplier ** attempt), max_backoff)

            worst_case_parallelism += capture_budget / max(1.0, poll_interval)

        estimated = int(math.ceil(worst_case_parallelism * 1.2))
        return max(1, min(self.num_streams, estimated))

    def shutdown(self, signum=None, frame=None):
        del signum, frame
        if not self.running:
            return

        self.running = False
        log_status("System", "N/A", "Shutdown signal received, draining workers.")

        if self._api_server is not None:
            self._api_server.should_exit = True

    def start(self):
        signal.signal(signal.SIGINT, self.shutdown)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, self.shutdown)

        log_status(
            "System",
            "N/A",
            (
                f"Engine started with {self.num_streams} streams, poll={self.default_poll_interval:.1f}s, "
                f"workers={self.max_workers}, recommended_workers={self.recommended_workers}."
            ),
        )

        if self.api_enabled:
            self._start_api_server()

        try:
            while self.running:
                self._dispatch_due_streams()
                time.sleep(self.dispatch_sleep_seconds)
        finally:
            self._stop_components()

    def _dispatch_due_streams(self):
        now = time.monotonic()
        for state in self.stream_state.values():
            if state.in_flight:
                continue
            if now < state.next_run_monotonic:
                continue

            state.in_flight = True
            state.next_run_monotonic = now + state.poll_interval
            future = self.pool.submit(self.check_stream, state.name)
            future.add_done_callback(partial(self._on_stream_future_done, state.name))

    def _on_stream_future_done(self, stream_name, future):
        state = self.stream_state.get(stream_name)
        if state is not None:
            state.in_flight = False

        exc = future.exception()
        if exc is not None:
            logger.opt(exception=exc).error("Unhandled worker exception for stream '{}'.", stream_name)

    def check_stream(self, stream_name):
        if not self.running:
            return

        state = self.stream_state[stream_name]

        if not state.lock.acquire(blocking=False):
            return

        started_at = time.monotonic()
        try:
            self._process_stream(state)
        except Exception as exc:
            logger.opt(exception=exc).error("Worker crashed for stream '{}'.", stream_name)
            state.consecutive_failures += 1
            self.status_store.update(
                stream_name,
                None,
                "STREAM_DOWN",
                logo=state.logo_url,
                error=f"worker crash: {exc}",
                retries=0,
                consecutive_failures=state.consecutive_failures,
            )
        finally:
            state.lock.release()
            elapsed = time.monotonic() - started_at
            if elapsed > state.poll_interval:
                logger.warning(
                    "Stream '{}' processing time {:.2f}s exceeded poll interval {:.2f}s.",
                    stream_name,
                    elapsed,
                    state.poll_interval,
                )

    def _process_stream(self, state):
        frame, error, retries_used = self._capture_with_retry(state)

        score = None
        frame_hash = None
        if frame is not None:
            self._save_roi_crop(frame, state)
            score = state.detector.process_frame(frame)
            frame_hash = compute_perceptual_hash(frame)

        status, score_text = state.state_machine.update(frame_hash, score, error)

        if error:
            state.consecutive_failures += 1
            level = "WARNING"
        else:
            state.consecutive_failures = 0
            level = "INFO"

        self.status_store.update(
            state.name,
            score,
            status,
            logo=state.logo_url,
            error=error,
            retries=retries_used,
            consecutive_failures=state.consecutive_failures,
        )

        details = f"{status} | retries={retries_used}"
        if error:
            details = f"{details} | error={error}"
        log_status(state.name, score_text, details, level=level)

    def _capture_with_retry(self, state):
        attempts_total = state.retry_attempts + 1
        last_error = None

        for attempt_index in range(attempts_total):
            if not self.running:
                return None, "scheduler stopping", attempt_index

            timeout = max(1.0, state.ffmpeg_timeout * (0.75 ** attempt_index))
            frame, error = state.reader.capture_frame(timeout=timeout)
            if frame is not None and not error:
                return frame, None, attempt_index

            last_error = error or "capture failed"
            retries_left = attempts_total - attempt_index - 1
            if retries_left <= 0:
                break

            delay = min(
                self.retry_backoff_seconds * (self.retry_backoff_multiplier ** attempt_index),
                self.retry_max_backoff_seconds,
            )
            logger.warning(
                "{} capture failed (attempt {}/{}): {}. Retrying in {:.2f}s.",
                state.name,
                attempt_index + 1,
                attempts_total,
                last_error,
                delay,
            )
            time.sleep(delay)

        return None, last_error, state.retry_attempts

    def _save_roi_crop(self, frame, state):
        roi = state.detector.roi
        if not roi:
            return

        h_img, w_img = frame.shape[:2]
        x = max(0, min(int(roi.get("x", 0)), w_img - 1))
        y = max(0, min(int(roi.get("y", 0)), h_img - 1))
        w = max(1, int(roi.get("width", 1)))
        h = max(1, int(roi.get("height", 1)))

        x2 = max(0, min(x + w, w_img))
        y2 = max(0, min(y + h, h_img))
        if x2 <= x or y2 <= y:
            return

        crop = frame[y:y2, x:x2]
        if crop.size == 0:
            return

        index = state.roi_counter % state.roi_max_frames
        path = os.path.join("roi", f"{state.name}_{index + 1:02d}.jpg")
        cv2.imwrite(path, crop)
        state.roi_counter += 1

    def _start_api_server(self):
        app = create_app(self.status_store, web_root="web", logos_root="logos")
        config = uvicorn.Config(
            app,
            host=self.api_host,
            port=self.api_port,
            log_level="warning",
            access_log=False,
        )
        self._api_server = uvicorn.Server(config)
        self._api_thread = threading.Thread(target=self._api_server.run, daemon=True, name="api-server")
        self._api_thread.start()
        log_status("System", "N/A", f"API server listening on http://{self.api_host}:{self.api_port}")

    def _stop_components(self):
        if self._api_server is not None:
            self._api_server.should_exit = True

        self.pool.shutdown(wait=True, cancel_futures=False)

        if self._api_thread is not None and self._api_thread.is_alive():
            self._api_thread.join(timeout=5)

        log_status("System", "N/A", "Scheduler stopped.")


def _resolve_config_path():
    env_override = os.getenv("LOGO_DETECTOR_CONFIG")
    if env_override:
        return env_override

    default_cfg = Path("config.yaml")
    if default_cfg.exists():
        return str(default_cfg)

    if getattr(sys, "frozen", False):
        exe_dir_cfg = Path(sys.executable).resolve().parent / "config.yaml"
        if exe_dir_cfg.exists():
            return str(exe_dir_cfg)

    return "config.yaml"


if __name__ == "__main__":
    setup_logger()

    os.makedirs("logos", exist_ok=True)
    os.makedirs("logs", exist_ok=True)
    os.makedirs("roi", exist_ok=True)

    try:
        config_path = _resolve_config_path()
        scheduler = Scheduler(config_path)
        scheduler.start()
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        logger.opt(exception=exc).error("CRITICAL CRASH")
        raise
