# Technical Architecture and Code Flow

This document describes runtime behavior of Missing Logo Detector for 24x7 NOC operations.

## Design Priorities

1. Fast missing-logo detection
2. High detection confidence before RED escalation
3. Continuous monitoring without deadlocks/process buildup
4. Immediate dashboard awareness
5. Non-blocking reporting/logging

## Main Runtime Components

- `main.py`
  - Initializes scheduler, stream runtime states, worker pool, and API thread
  - Runs autonomous restart loop around scheduler start
- `ffmpeg_reader.py`
  - Persistent per-stream FFmpeg worker
  - Rawvideo frame pipe (`bgr24`) and watchdog-based self-heal restart
- `detector.py`
  - Template matching score computation only
  - ROI extraction/normalization
- `status_store.py`
  - Thread-safe state store
  - Per-channel timeline + stability aggregation
  - Condition-based update notifications for SSE
- `report_manager.py`
  - Transition-aware event recorder
  - Queue-based single writer for batched report writes
- `api.py`
  - REST endpoints + SSE stream endpoint
  - Static UI hosting + runtime logo serving

## Stream Lifecycle

1. Stream config is loaded into `StreamRuntimeState`.
2. FFmpeg reader starts and keeps newest frames in memory.
3. Scheduler dispatches `check_stream` jobs at per-stream poll interval.
4. Worker captures latest frame and computes template score.
5. State machine classifies stream state and emits status update.
6. Dashboard is updated instantly through store notification/SSE.

## Missing-Logo Confirmation Flow

- Trigger condition: detection enters missing path.
- Immediate UI signal: `LOGO MAYBE MISSING` (YELLOW).
- Recheck thread starts (non-blocking):
  - fixed 5 seconds
  - fixed 1 frame per second
- Finalization:
  - RED (`MISSING DETECTED`) if all successful checks remain missing
  - timeout/stream issue if captures fail throughout recheck
  - GREEN recovery if logo reappears (false alarm suppression)

## Recheck Deadlock Protection

- Runtime tracks `recheck_start_monotonic`.
- Dispatcher checks lock age each cycle.
- If lock age exceeds 15 seconds, recheck lock is force-reset.
- This prevents permanent freeze if a daemon recheck thread dies unexpectedly.

## International Schedule Handling

- XML schedules are parsed into timeline intervals.
- Material IDs map ON/OFF windows.
- OFF windows suppress missing-logo RED escalation.
- Store/API expose WHITE-style operational visibility for scheduled absence.

## Dashboard Data Path

- Backend pushes updates via `wait_for_updates(...)` condition notify.
- API exposes `GET /api/logo_status/stream` as SSE event feed.
- Frontend consumes SSE for near-immediate tile updates.
- Frontend falls back to periodic polling if SSE disconnects.

## Reporting Path

- `record_event()` only reacts to transitions (not every cycle).
- `generate_daily_report_async()` enqueues write trigger.
- Single worker batches updates and writes report at ~1s cadence.
- Reduces lock contention and thread spikes during mass channel transitions.

## Operational Status Mapping

- `GREEN`: stable/logo present
- `YELLOW`: potential issue, warmup, or verification phase
- `RED`: confirmed missing logo
- `WHITE`: intentional/scheduled logo-off state

## Resource Stability Model

- Persistent FFmpeg eliminates repeated process spawn overhead.
- Watchdog restarts stalled FFmpeg readers.
- Rawvideo decode path reduces CPU spent on PNG encode/decode cycles.
- Recheck logic does not block main scheduler dispatch loop.
- Report writing is decoupled from detection hot path.
