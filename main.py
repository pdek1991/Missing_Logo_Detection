import math
import os
import psutil
import fnmatch
import signal
import sys
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
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
from logo_schedule import LogoScheduleTimeline
from report_manager import ReportManager
from state_machine import StateMachine
from status_store import StatusStore
from utils import compute_perceptual_hash

DEFAULT_MAX_STREAMS = 50
DEFAULT_POLL_INTERVAL_SECONDS = 10.0
DEFAULT_LATENCY_SECONDS = 5.0
DEFAULT_MISSING_RECHECK_SECONDS = 5.0
DEFAULT_MISSING_RECHECK_FPS = 1.0
DEFAULT_MISSING_RECHECK_CAPTURE_TIMEOUT_SECONDS = 1.0
DEFAULT_XML_RECHECK_INTERVAL_SECONDS = 3600.0
DEFAULT_INTERNATIONAL_LOGO_ON_MATERIAL_IDS = ("*CI1001*",)
DEFAULT_INTERNATIONAL_LOGO_OFF_MATERIAL_IDS = ("*CI0000*",)
MAX_SCHEDULE_CACHE_SIZE = 100  # Eviction policy for memory safety


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
    channel_type: str
    latency_seconds: float
    missing_recheck_seconds: float
    missing_recheck_fps: float
    missing_recheck_capture_timeout_seconds: float
    schedule_xml_prefix: str = ""
    schedule_xml_pattern: str = ""
    schedule_xml_dir: str = "channel xml"
    xml_recheck_interval_seconds: float = DEFAULT_XML_RECHECK_INTERVAL_SECONDS
    schedule_next_refresh_monotonic: float = 0.0
    international_logo_on_material_ids: tuple = ()
    international_logo_off_material_ids: tuple = ()
    schedule_timeline: LogoScheduleTimeline = None
    schedule_source_path: str = None
    schedule_source_mtime: float = 0.0
    schedule_warning_logged: bool = False
    lock: threading.Lock = field(default_factory=threading.Lock)
    in_flight: bool = False
    is_rechecking: bool = False
    recheck_start_monotonic: float = 0.0
    consecutive_failures: int = 0
    roi_counter: int = 0
    roi_max_frames: int = 5
    secondary_roi: dict = None


class Scheduler:
    def __init__(self, config_file):
        with open(config_file, "r", encoding="utf-8") as handle:
            self.config = yaml.safe_load(handle) or {}

        self.streams = self.config.get("streams") or []
        self.num_streams = len(self.streams)

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
        self.default_latency_seconds = float(scheduler_cfg.get("default_latency_seconds", DEFAULT_LATENCY_SECONDS))
        self.default_missing_recheck_seconds = float(
            scheduler_cfg.get("default_missing_recheck_seconds", DEFAULT_MISSING_RECHECK_SECONDS)
        )
        self.default_missing_recheck_fps = float(
            scheduler_cfg.get("default_missing_recheck_fps", DEFAULT_MISSING_RECHECK_FPS)
        )
        self.default_missing_recheck_capture_timeout_seconds = float(
            scheduler_cfg.get(
                "default_missing_recheck_capture_timeout_seconds",
                DEFAULT_MISSING_RECHECK_CAPTURE_TIMEOUT_SECONDS,
            )
        )
        self.default_xml_recheck_interval_seconds = float(
            scheduler_cfg.get(
                "xml_recheck_interval_seconds",
                scheduler_cfg.get("default_schedule_refresh_seconds", DEFAULT_XML_RECHECK_INTERVAL_SECONDS),
            )
        )
        self.default_international_logo_on_material_ids = self._normalize_material_id_list(
            scheduler_cfg.get("default_international_logo_on_material_ids"),
            fallback=DEFAULT_INTERNATIONAL_LOGO_ON_MATERIAL_IDS,
            setting_name="scheduler.default_international_logo_on_material_ids",
        )
        self.default_international_logo_off_material_ids = self._normalize_material_id_list(
            scheduler_cfg.get("default_international_logo_off_material_ids"),
            fallback=DEFAULT_INTERNATIONAL_LOGO_OFF_MATERIAL_IDS,
            setting_name="scheduler.default_international_logo_off_material_ids",
        )

        # Performance / Heavy IO Settings
        self.debug_roi_enabled = bool(scheduler_cfg.get("debug_roi_enabled", False))
        self.default_roi_max_frames = int(scheduler_cfg.get("roi_max_frames", 5))
        self.web_refresh_interval = int(scheduler_cfg.get("web_refresh_interval_seconds", 300))
        self.resource_monitor_interval_seconds = max(
            5.0, float(scheduler_cfg.get("resource_monitor_interval_seconds", 300.0))
        )
        self.resource_monitor_cpu_threshold_percent = max(
            1.0, min(100.0, float(scheduler_cfg.get("resource_monitor_cpu_threshold_percent", 50.0)))
        )
        self.resource_monitor_ram_threshold_percent = max(
            1.0, min(100.0, float(scheduler_cfg.get("resource_monitor_ram_threshold_percent", 50.0)))
        )
        self.resource_monitor_zombie_threshold = max(
            0, int(scheduler_cfg.get("resource_monitor_zombie_threshold", 10))
        )

        self.max_workers = int(scheduler_cfg.get("max_workers", 50))
        self.recommended_workers = self._calculate_recommended_workers()

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

        # Memory safety: Use OrderedDict to implement LRU eviction for schedule cache
        self._schedule_cache = OrderedDict()
        self._schedule_lock = threading.Lock()

        self._api_server = None
        self._api_thread = None
        self.report_manager = ReportManager()

        self._initialize_stream_states()

    def _normalize_material_id_list(self, raw_value, fallback, setting_name):
        values = raw_value
        if values is None:
            values = fallback
        elif isinstance(values, str):
            values = [values]
        elif not isinstance(values, (list, tuple, set)):
            logger.warning(
                "Invalid '{}' type '{}'; using fallback {}.",
                setting_name,
                type(raw_value).__name__,
                fallback,
            )
            values = fallback

        normalized = []
        seen = set()
        for item in values:
            token = str(item).strip()
            if not token or token in seen:
                continue
            seen.add(token)
            normalized.append(token)

        if not normalized:
            normalized = [str(item).strip() for item in fallback if str(item).strip()]
        return tuple(normalized)

    def _initialize_stream_states(self):
        for stream_cfg in self.streams[: self.max_streams]:
            name = stream_cfg.get("name", "Unknown")
            url = stream_cfg.get("url")
            if not url:
                logger.error("Stream '{}' has no URL, skipping.", name)
                continue

            template_path = stream_cfg.get("template_path")
            if not template_path:
                logger.error("Stream '{}' has no template_path, skipping.", name)
                continue

            roi = stream_cfg.get("roi")
            detector = LogoDetector(template_path, roi=roi)

            # Check for missing template at startup
            if not detector.template_loaded:
                logger.warning(
                    "Stream '{}': Template file not found or invalid ({}). Flagging as MISSING TEMPLATE.",
                    name,
                    template_path,
                )

            ffmpeg_timeout = float(stream_cfg.get("ffmpeg_timeout_seconds", self.default_ffmpeg_timeout))
            reader = FFmpegReader(url, timeout_seconds=ffmpeg_timeout)
            reader.start()
            state_machine = StateMachine(stream_cfg)

            poll_interval = float(
                stream_cfg.get(
                    "poll_interval_seconds",
                    stream_cfg.get("scan_cycle_seconds", self.default_poll_interval),
                )
            )

            latency = float(stream_cfg.get("latency_seconds", self.default_latency_seconds))
            recheck_seconds = float(stream_cfg.get("missing_recheck_seconds", self.default_missing_recheck_seconds))
            recheck_fps = float(stream_cfg.get("missing_recheck_fps", self.default_missing_recheck_fps))
            recheck_capture_timeout = float(
                stream_cfg.get(
                    "missing_recheck_capture_timeout_seconds",
                    self.default_missing_recheck_capture_timeout_seconds,
                )
            )

            state = StreamRuntimeState(
                name=name,
                config=stream_cfg,
                reader=reader,
                detector=detector,
                state_machine=state_machine,
                poll_interval=poll_interval,
                ffmpeg_timeout=ffmpeg_timeout,
                retry_attempts=int(stream_cfg.get("retry_attempts", self.default_retry_attempts)),
                next_run_monotonic=time.monotonic() + (len(self.stream_state) * 0.5),
                logo_url=self._resolve_logo_url(stream_cfg),
                channel_type=stream_cfg.get("channel_type", "india"),
                latency_seconds=latency,
                missing_recheck_seconds=recheck_seconds,
                missing_recheck_fps=recheck_fps,
                missing_recheck_capture_timeout_seconds=recheck_capture_timeout,
                schedule_xml_prefix=stream_cfg.get("schedule_xml_prefix", ""),
                schedule_xml_pattern=stream_cfg.get("schedule_xml_pattern", ""),
                schedule_xml_dir=stream_cfg.get("schedule_xml_dir", "channel xml"),
                xml_recheck_interval_seconds=float(
                    stream_cfg.get(
                        "xml_recheck_interval_seconds",
                        stream_cfg.get("schedule_refresh_seconds", self.default_xml_recheck_interval_seconds),
                    )
                ),
                international_logo_on_material_ids=self._normalize_material_id_list(
                    stream_cfg.get("international_logo_on_material_ids"),
                    fallback=self.default_international_logo_on_material_ids,
                    setting_name=f"streams[{name}].international_logo_on_material_ids",
                ),
                international_logo_off_material_ids=self._normalize_material_id_list(
                    stream_cfg.get("international_logo_off_material_ids"),
                    fallback=self.default_international_logo_off_material_ids,
                    setting_name=f"streams[{name}].international_logo_off_material_ids",
                ),
                roi_max_frames=int(stream_cfg.get("roi_max_frames", self.default_roi_max_frames)),
                secondary_roi=stream_cfg.get("secondary_roi"),
            )
            self.stream_state[name] = state
            self.status_store.register_stream(name, state.logo_url)

    def _load_schedule_timeline(self, state):
        now_monotonic = time.monotonic()
        if (
            state.schedule_timeline
            and now_monotonic < state.schedule_next_refresh_monotonic
            and state.schedule_source_path
        ):
            # Check file mtime only occasionally to avoid heavy OS calls
            try:
                mtime = os.path.getmtime(state.schedule_source_path)
                if mtime <= state.schedule_source_mtime:
                    return state.schedule_timeline
            except OSError:
                pass

        resolved = self._resolve_schedule_path(state)
        if not resolved:
            if not state.schedule_warning_logged:
                schedule_pattern = (state.schedule_xml_pattern or f"{state.schedule_xml_prefix}*.xml").strip()
                logger.warning(
                    "No schedule XML found for '{}' using pattern '{}' in '{}'.",
                    state.name,
                    schedule_pattern,
                    state.schedule_xml_dir,
                )
                state.schedule_warning_logged = True
            return None

        state.schedule_warning_logged = False
        mtime = os.path.getmtime(resolved)

        # Check LRU cache
        # Include material ID patterns in cache key to prevent wrong reuse
        cache_key = (
            resolved,
            mtime,
            tuple(sorted(state.international_logo_on_material_ids)),
            tuple(sorted(state.international_logo_off_material_ids)),
        )
        with self._schedule_lock:
            if cache_key in self._schedule_cache:
                # Move to end (most recently used)
                self._schedule_cache.move_to_end(cache_key)
                state.schedule_timeline = self._schedule_cache[cache_key]
                state.schedule_source_path = resolved
                state.schedule_source_mtime = mtime
                state.schedule_next_refresh_monotonic = now_monotonic + state.xml_recheck_interval_seconds
                return state.schedule_timeline

        # Not in cache or stale, load it
        logger.debug("Loading schedule XML for '{}': {}", state.name, resolved)
        try:
            timeline = LogoScheduleTimeline.from_xml(
                resolved,
                on_material_ids=state.international_logo_on_material_ids,
                off_material_ids=state.international_logo_off_material_ids,
            )
        except Exception as exc:
            logger.error("Failed to load schedule XML for '{}': {}", state.name, exc)
            return None

        # Add to cache with eviction
        with self._schedule_lock:
            self._schedule_cache[cache_key] = timeline
            if len(self._schedule_cache) > MAX_SCHEDULE_CACHE_SIZE:
                self._schedule_cache.popitem(last=False)  # Remove oldest

        state.schedule_timeline = timeline
        state.schedule_source_path = resolved
        state.schedule_source_mtime = mtime
        state.schedule_next_refresh_monotonic = now_monotonic + state.xml_recheck_interval_seconds
        return timeline

    def _resolve_schedule_path(self, state):
        pattern = (state.schedule_xml_pattern or "").strip()
        if not pattern:
            if not state.schedule_xml_prefix:
                return None
            pattern = f"{state.schedule_xml_prefix}*.xml"

        try:
            files = os.listdir(state.schedule_xml_dir)
            matches = [f for f in files if fnmatch.fnmatch(f.lower(), pattern.lower())]
            if not matches:
                return None

            # Use latest modified file instead of lexicographic ordering.
            resolved_paths = [os.path.join(state.schedule_xml_dir, name) for name in matches]
            resolved_paths.sort(key=lambda p: os.path.getmtime(p), reverse=True)
            return resolved_paths[0]
        except OSError:
            return None

    def _get_logo_score(self, frame, state):
        if frame is None:
            return None

        # Check if template matching is even possible
        if not state.detector.template_loaded:
            # Returning a value that allows StateMachine to identify the MISSING TEMPLATE state
            return -1.0  # Special indicator for missing template

        # Template matching only.
        def evaluate_roi(roi_region=None):
            template_score = state.detector.process_frame(frame, roi_region=roi_region)
            if template_score is None:
                return None

            return max(0.0, min(1.0, float(template_score)))

        # Primary evaluation on configured ROI.
        score = evaluate_roi()

        # Secondary Logic: Sports Channel check secondary ROI
        if state.channel_type == "sports" and state.secondary_roi:
            threshold_red = float(state.state_machine.threshold_red)
            if score is None or score < threshold_red:
                sec_score = evaluate_roi(roi_region=state.secondary_roi)
                if sec_score is not None and (score is None or sec_score > score):
                    score = sec_score
        return score

    def _process_stream(self, state):
        process_start = time.monotonic()
        frame, error, retries_used = self._capture_with_retry(state)

        score = None
        frame_hash = None
        if frame is not None:
            if self.debug_roi_enabled:
                self._save_roi_crop(frame, state)
            score = self._get_logo_score(frame, state)
            frame_hash = compute_perceptual_hash(frame)

        # Handle special score indicator for missing template
        status = None
        score_text = "N/A"
        if score == -1.0:
            status = "MISSING TEMPLATE"
            score = None
        else:
            status, score_text = state.state_machine.update(frame_hash, score, error)

        detail_suffix = None

        if not error and status in {"LOGO MISSING", "LOGO MAYBE MISSING"}:
            skip_detection, adjusted_time, schedule_decision = self._should_skip_missing_by_schedule(state)
            if skip_detection:
                status = "INTENTIONAL BREAK"
                if schedule_decision and schedule_decision.off_window:
                    detail_suffix = (
                        f"xml_off_window={schedule_decision.off_window.start.isoformat()}"
                        f"->{schedule_decision.off_window.end.isoformat()}"
                    )
                else:
                    detail_suffix = f"xml_off_window={adjusted_time.isoformat()}"
            else:
                # Do not change dashboard state immediately for suspected missing logos.
                # Run the fixed 5-second (1 FPS) confirmation first, then publish final state.
                log_status(
                    state.name,
                    score_text,
                    f"{status} SUSPECTED | retries={retries_used} | waiting_for_recheck",
                    level="WARNING",
                )

                state.is_rechecking = True
                state.recheck_start_monotonic = time.monotonic()
                threading.Thread(
                    target=self._run_non_blocking_recheck,
                    args=(state, score, schedule_decision, retries_used),
                    daemon=True,
                    name=f"recheck-{state.name}"
                ).start()
                return  # Processing continues in background thread

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

        # Track incidents and generate daily report on status transitions ONLY (avoids heavy I/O every cycle)
        if self.report_manager.record_event(state.name, status):
            self.report_manager.generate_daily_report_async()

        details = f"{status} | retries={retries_used}"
        if detail_suffix:
            details = f"{details} | {detail_suffix}"
        if error:
            details = f"{details} | error={error}"

        processing_time = time.monotonic() - process_start
        warn_threshold = max(state.poll_interval * 1.8, 1.5)
        if processing_time > warn_threshold:
            logger.warning(
                "Stream '{}' processing time {:.2f}s exceeded warning threshold {:.2f}s (poll {:.2f}s).",
                state.name,
                processing_time,
                warn_threshold,
                state.poll_interval,
            )

        log_status(state.name, score_text, details, level=level)

    def _should_skip_missing_by_schedule(self, state):
        if state.channel_type != "international":
            return False, datetime.now(timezone.utc), None

        timeline = self._load_schedule_timeline(state)
        if timeline is None:
            return False, datetime.now(timezone.utc), None

        now_local = datetime.now().astimezone()
        skip, adjusted_time = timeline.should_skip_logo_detection(
            current_time=now_local,
            latency_seconds=state.latency_seconds,
        )
        return skip, adjusted_time, timeline.last_decision

    def _confirm_missing_logo(self, state, max_duration_seconds=None):
        # Fixed confirmation window: 5 seconds at 1 FPS.
        del max_duration_seconds
        fps = 1.0
        seconds = 5.0
        total_checks = max(1, int(round(seconds * fps)))
        interval_seconds = 1.0 / fps

        threshold_red = float(state.state_machine.threshold_red)
        capture_timeout_cap = float(state.missing_recheck_capture_timeout_seconds)

        window_start = time.monotonic()
        window_end = window_start + seconds

        missing_count = 0
        checks_done = 0
        capture_errors = 0

        for i in range(total_checks):
            if not self.running:
                break

            target_time = window_start + (i * interval_seconds)
            now = time.monotonic()
            if target_time > now:
                time.sleep(target_time - now)

            now = time.monotonic()
            remaining_window = window_end - now
            if remaining_window <= 0:
                break

            capture_timeout = max(0.2, min(capture_timeout_cap, remaining_window))
            frame, capture_error = state.reader.capture_frame(timeout=capture_timeout)
            checks_done += 1

            if capture_error or frame is None:
                capture_errors += 1
                log_status(
                    state.name,
                    "N/A",
                    f"RECHECK {checks_done}/{total_checks} | capture_error={capture_error or 'no frame'}",
                    level="WARNING",
                )
                continue

            score = self._get_logo_score(frame, state)
            if score is None or score < threshold_red:
                missing_count += 1
                log_status(
                    state.name,
                    f"{score:.2f}" if score is not None else "N/A",
                    f"RECHECK {checks_done}/{total_checks} | missing=true",
                    level="WARNING",
                )
            else:
                log_status(
                    state.name,
                    f"{score:.2f}",
                    f"RECHECK {checks_done}/{total_checks} | missing=false",
                    level="INFO",
                )

        successful_checks = max(0, checks_done - capture_errors)
        confirmed = successful_checks > 0 and capture_errors == 0 and missing_count == successful_checks
        return confirmed, missing_count, checks_done, capture_errors

    def _run_non_blocking_recheck(self, state, initial_score, schedule_decision, retries_used):
        try:
            confirmed, missing_count, checks_done, capture_errors = self._confirm_missing_logo(
                state,
                max_duration_seconds=state.missing_recheck_seconds,
            )

            detail_suffix = f"recheck={missing_count}/{checks_done}"
            if confirmed:
                status = "MISSING DETECTED"
                state.consecutive_failures += 1
            elif checks_done > 0 and capture_errors == checks_done:
                status = "STREAM TIMEOUT"
                state.consecutive_failures += 1
                detail_suffix += " | stream_died_during_recheck"
            else:
                status = "LOGO PRESENT"
                state.state_machine.state = "LOGO PRESENT"
                state.state_machine.consecutive_failures = 0
                state.consecutive_failures = 0
                detail_suffix += " | false_alarm=true"

            on_context = None
            if schedule_decision and schedule_decision.on_interval:
                on_context = (
                    f"xml_on_window={schedule_decision.on_interval.start.isoformat()}"
                    f"->{schedule_decision.on_interval.end.isoformat()}"
                )
            if on_context:
                detail_suffix = f"{detail_suffix} | {on_context}"

            self.status_store.update(
                state.name,
                initial_score,
                status,
                logo=state.logo_url,
                error=None,
                retries=retries_used,
                consecutive_failures=state.consecutive_failures,
            )

            if self.report_manager.record_event(state.name, status):
                self.report_manager.generate_daily_report_async()

            log_status(
                state.name,
                f"{initial_score:.2f}" if initial_score is not None else "N/A",
                f"{status} | {detail_suffix}",
            )
        except Exception as exc:
            logger.opt(exception=exc).error("Recheck crashed for stream '{}'.", state.name)
        finally:
            state.is_rechecking = False
            state.recheck_start_monotonic = 0.0

    def _capture_with_retry(self, state):
        attempts_total = state.retry_attempts + 1
        last_error = None
        start = time.monotonic()
        # Keep single cycle bounded close to poll interval to avoid backlog/flicker.
        max_cycle_budget = max(0.8, state.poll_interval * 1.2)

        for attempt_index in range(attempts_total):
            if not self.running:
                return None, "scheduler stopping", attempt_index

            elapsed = time.monotonic() - start
            remaining = max_cycle_budget - elapsed
            if remaining <= 0.0:
                break

            nominal_timeout = max(0.2, state.ffmpeg_timeout * (0.75 ** attempt_index))
            per_attempt_budget = max(0.2, remaining / float(max(1, attempts_total - attempt_index)))
            timeout = min(nominal_timeout, per_attempt_budget)
            frame, error = state.reader.capture_frame(timeout=timeout)
            if frame is not None and not error:
                return frame, None, attempt_index

            last_error = error or "capture failed"
            retries_left = attempts_total - attempt_index - 1
            if retries_left <= 0:
                break

            nominal_delay = min(
                self.retry_backoff_seconds * (self.retry_backoff_multiplier ** attempt_index),
                self.retry_max_backoff_seconds,
            )
            elapsed = time.monotonic() - start
            remaining = max_cycle_budget - elapsed
            if remaining <= 0.2:
                break

            delay = min(nominal_delay, max(0.0, remaining - 0.2))
            if delay <= 0.0:
                continue

            logger.warning(
                "{} capture failed (attempt {}/{}): {}. Retrying in {:.2f}s.",
                state.name,
                attempt_index + 1,
                attempts_total,
                last_error,
                delay,
            )
            time.sleep(delay)

        return None, last_error, attempts_total - 1

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
        scheduler_cfg = self.config.get("scheduler", {})
        default_poll = max(1.0, float(scheduler_cfg.get("default_poll_interval_seconds", DEFAULT_POLL_INTERVAL_SECONDS)))
        default_timeout = float(scheduler_cfg.get("default_ffmpeg_timeout_seconds", 8.0))
        default_retries = int(scheduler_cfg.get("default_retry_attempts", 2))
        backoff = float(scheduler_cfg.get("retry_backoff_seconds", 1.0))
        
        # Target formula: Workers = (TotalStreams × worst_case_processing_time / poll_interval) × 1.25
        # Recheck is now non-blocking, so worst case is just original captures + retries
        worst_case_processing_time = default_timeout + (default_retries * backoff)
        estimated = int(math.ceil((self.num_streams * worst_case_processing_time / default_poll) * 1.25))
        
        # Ensures that degraded streams have sufficient compute capacity without freezing thread pool.
        return max(1, estimated)

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

        threading.Thread(target=self._resource_monitor_thread, daemon=True, name="resource-monitor").start()

        if self.api_enabled:
            self._start_api_server()

        try:
            while self.running:
                # API Thread Health Monitoring
                if self.api_enabled and (self._api_thread is None or not self._api_thread.is_alive()):
                    logger.error("API server thread died, restarting...")
                    self._start_api_server()

                self._dispatch_due_streams()
                time.sleep(self.dispatch_sleep_seconds)
        finally:
            self._stop_components()

    def _dispatch_due_streams(self):
        now = time.monotonic()
        for state in self.stream_state.values():
            recheck_start = float(getattr(state, "recheck_start_monotonic", 0.0) or 0.0)
            if recheck_start > 0.0:
                elapsed = now - recheck_start
                if elapsed > 15.0:
                    logger.error(
                        "Recheck lock stale for '{}': {:.1f}s. Forcing reset.",
                        state.name,
                        elapsed,
                    )
                    state.is_rechecking = False
                    state.recheck_start_monotonic = 0.0
                else:
                    continue

            if state.in_flight or getattr(state, "is_rechecking", False):
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

    def _stop_components(self):
        if self._api_server is not None:
            self._api_server.should_exit = True

        for state in self.stream_state.values():
            try:
                state.reader.stop()
            except Exception as exc:
                logger.error("Failed stopping FFmpeg reader for '{}': {}", state.name, exc)

        self.pool.shutdown(wait=True, cancel_futures=False)

        if self._api_thread is not None and self._api_thread.is_alive():
            self._api_thread.join(timeout=5)

        try:
            self.report_manager.shutdown()
        except Exception as exc:
            logger.error("Failed stopping report manager: {}", exc)

        log_status("System", "N/A", "Scheduler stopped.")

    def _start_api_server(self):
        config_data = {
            "web_refresh_interval_seconds": self.web_refresh_interval,
        }
        app = create_app(self.status_store, web_root="web", logos_root="logos", config_data=config_data)
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

    def _resource_monitor_thread(self):
        process = psutil.Process(os.getpid())
        psutil.cpu_percent(interval=None)  # Prime CPU measurement baseline.

        while self.running:
            time.sleep(self.resource_monitor_interval_seconds)
            if not self.running:
                break

            try:
                sys_cpu = psutil.cpu_percent(interval=None)
                sys_mem = psutil.virtual_memory().percent
                zombie_count = sum(
                    1
                    for child in process.children(recursive=True)
                    if child.status() == psutil.STATUS_ZOMBIE
                )

                if (
                    sys_cpu > self.resource_monitor_cpu_threshold_percent
                    or sys_mem > self.resource_monitor_ram_threshold_percent
                    or zombie_count > self.resource_monitor_zombie_threshold
                ):
                    logger.error(
                        "System utilization threshold crossed; exiting process immediately. "
                        "Sys CPU: {:.1f}% (limit {:.1f}%), Sys Mem: {:.1f}% (limit {:.1f}%), "
                        "Zombies: {} (limit {}).",
                        sys_cpu,
                        self.resource_monitor_cpu_threshold_percent,
                        sys_mem,
                        self.resource_monitor_ram_threshold_percent,
                        zombie_count,
                        self.resource_monitor_zombie_threshold,
                    )
                    os._exit(1)
            except Exception as exc:
                logger.opt(exception=exc).error("Resource monitor check failed.")


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

    while True:
        try:
            config_path = _resolve_config_path()
            logger.info("Starting Logo Detector Scheduler (Autonomous Restart Loop)...")
            scheduler = Scheduler(config_path)
            scheduler.start()
            # If start() returns normally, the user probably stopped it with SIGINT
            break
        except KeyboardInterrupt:
            logger.warning("KeyboardInterrupt received. Exiting.")
            break
        except Exception as exc:
            logger.opt(exception=exc).error("CRITICAL ENGINE CRASH. Restarting in 10s...")
            time.sleep(10)
