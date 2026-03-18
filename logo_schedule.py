import bisect
import fnmatch
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


@dataclass(frozen=True)
class Interval:
    start: datetime
    end: datetime


@dataclass(frozen=True)
class LogoMarker:
    time: datetime
    order: int
    material_id: str
    is_on: bool
    is_off: bool


@dataclass(frozen=True)
class ScheduleDecision:
    adjusted_time: datetime
    expected_on: bool
    intentional_off: bool
    on_interval: Interval = None
    off_window: Interval = None
    reason: str = ""


def _parse_duration(value):
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    parts = text.split(":")
    if len(parts) != 3:
        return None

    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        sec_part = parts[2]
        if "." in sec_part:
            seconds_text, frac_text = sec_part.split(".", 1)
        else:
            seconds_text, frac_text = sec_part, ""

        seconds = int(seconds_text)
        micro = int((frac_text + "000000")[:6]) if frac_text else 0
    except ValueError:
        return None

    return timedelta(hours=hours, minutes=minutes, seconds=seconds, microseconds=micro)


def _parse_iso(value):
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"

    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _combine_date_and_time(date_value, time_value):
    base = _parse_iso(date_value)
    offset = _parse_duration(time_value)
    if base is None or offset is None:
        return None

    midnight = datetime(base.year, base.month, base.day, tzinfo=base.tzinfo)
    return midnight + offset


class LogoScheduleTimeline:
    def __init__(self, source_path, on_intervals, off_intervals, markers=None):
        self.source_path = str(source_path)
        self.on_intervals = tuple(on_intervals)
        self.off_intervals = tuple(off_intervals)
        self.markers = tuple(sorted(markers or (), key=lambda item: (item.time, item.order)))
        self._on_starts = [interval.start for interval in self.on_intervals]
        self._off_starts = [interval.start for interval in self.off_intervals]
        self._marker_starts = [marker.time for marker in self.markers]
        self.last_decision = None

    @classmethod
    def from_xml(cls, xml_path, on_material_ids=None, off_material_ids=None):
        path = Path(xml_path)
        on_ids = cls._normalize_material_ids(on_material_ids, fallback=("*CI1001*",))
        off_ids = cls._normalize_material_ids(off_material_ids, fallback=("*CI0000*",))
        raw_on_intervals = []
        ci_markers = []
        current_event_start = None
        local_now = datetime.now().astimezone()
        inferred_date = local_now.date()
        inferred_tz = local_now.tzinfo
        last_time_offset = None
        marker_order = 0

        for _, elem in ET.iterparse(path, events=("end",)):
            if elem.tag != "Event":
                elem.clear()
                continue

            event_type = str(elem.get("Type") or "").strip().upper()
            if event_type != "LOGO":
                start_time, inferred_date, last_time_offset = cls._resolve_event_start(
                    elem,
                    inferred_date,
                    inferred_tz,
                    last_time_offset,
                )
                if start_time is not None:
                    current_event_start = start_time
                elem.clear()
                continue

            material_id = cls._normalize_material_token(elem.get("MaterialID"))
            if not material_id:
                start_time, inferred_date, last_time_offset = cls._resolve_event_start(
                    elem,
                    inferred_date,
                    inferred_tz,
                    last_time_offset,
                )
                if start_time is not None:
                    current_event_start = start_time
                elem.clear()
                continue

            is_on_marker = cls._matches_any_pattern(material_id, on_ids)
            is_off_marker = cls._matches_any_pattern(material_id, off_ids)
            if not is_on_marker and not is_off_marker:
                # Some playlists may carry Date/Time on non-CI LOGO rows.
                # Keep the nearest parseable event-above anchor for later CI rows.
                start_time, inferred_date, last_time_offset = cls._resolve_event_start(
                    elem,
                    inferred_date,
                    inferred_tz,
                    last_time_offset,
                )
                if start_time is not None:
                    current_event_start = start_time
                elem.clear()
                continue

            marker_time = cls._resolve_logo_start(elem, current_event_start)
            if marker_time is not None:
                ci_markers.append(
                    LogoMarker(
                        time=marker_time,
                        order=marker_order,
                        material_id=material_id,
                        is_on=is_on_marker,
                        is_off=is_off_marker,
                    )
                )
                marker_order += 1

                if is_on_marker:
                    interval = cls._build_ci_interval(elem, marker_time)
                    if interval is not None:
                        raw_on_intervals.append(interval)

            elem.clear()

        on_intervals = cls._merge_intervals(raw_on_intervals)
        off_intervals = cls._build_off_intervals_from_markers(ci_markers)
        return cls(path, on_intervals, off_intervals, markers=ci_markers)

    def evaluate_time(self, adjusted_time):
        target = adjusted_time
        if target.tzinfo is None:
            target = target.replace(tzinfo=timezone.utc)

        # Use the nearest marker above current time to evaluate OFF windows.
        off_window = self._find_off_window_from_marker_context(target)
        if off_window is None:
            off_window = self._find_interval(target, self.off_intervals, self._off_starts)
        if off_window is not None:
            decision = ScheduleDecision(
                adjusted_time=target,
                expected_on=False,
                intentional_off=True,
                off_window=off_window,
                reason="inside_logo_off_window",
            )
            self.last_decision = decision
            return decision

        on_interval = self._find_interval(target, self.on_intervals, self._on_starts)
        if on_interval is not None:
            decision = ScheduleDecision(
                adjusted_time=target,
                expected_on=True,
                intentional_off=False,
                on_interval=on_interval,
                reason="inside_logo_on_interval",
            )
            self.last_decision = decision
            return decision

        decision = ScheduleDecision(
            adjusted_time=target,
            expected_on=False,
            intentional_off=False,
            reason="outside_logo_on_interval",
        )
        self.last_decision = decision
        return decision

    def _find_off_window_from_marker_context(self, target):
        if not self.markers:
            return None

        upper_idx = bisect.bisect_right(self._marker_starts, target)
        if upper_idx <= 0:
            return None

        previous_marker = self.markers[upper_idx - 1]
        if not previous_marker.is_off:
            return None

        next_on_marker = self._find_next_on_marker(upper_idx)
        if next_on_marker is None:
            return None

        if next_on_marker.time <= previous_marker.time:
            return None

        if previous_marker.time <= target < next_on_marker.time:
            return Interval(start=previous_marker.time, end=next_on_marker.time)
        return None

    def _find_next_on_marker(self, start_index):
        if start_index < 0:
            start_index = 0
        for marker in self.markers[start_index:]:
            if marker.is_on:
                return marker
        return None

    def should_skip_logo_detection(self, current_time=None, latency_seconds=5.0):
        now = current_time or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        latency_seconds = max(0.0, float(latency_seconds))
        adjusted = now + timedelta(seconds=latency_seconds)
        decision = self.evaluate_time(adjusted)
        return decision.intentional_off, adjusted

    @staticmethod
    def _resolve_logo_start(elem, current_event_start):
        som_delta = _parse_duration(elem.get("SOM"))
        # Primary rule: use the schedule event above CI marker as time/date anchor.
        # CI logo events usually provide SOM relative to the enclosing program event.
        if current_event_start is not None:
            if som_delta is not None:
                return current_event_start + som_delta
            return current_event_start

        logo_time = _combine_date_and_time(elem.get("Date"), elem.get("Time"))
        if logo_time is not None:
            return logo_time

        if som_delta is None:
            return None

        event_date = _parse_iso(elem.get("Date"))
        if event_date is None:
            return None
        base_time = datetime(event_date.year, event_date.month, event_date.day, tzinfo=event_date.tzinfo)
        return base_time + som_delta

    @staticmethod
    def _build_ci_interval(elem, marker_time):
        duration = _parse_duration(elem.get("Duration"))
        if duration is None or duration.total_seconds() <= 0:
            return None

        if marker_time is None:
            return None

        return Interval(start=marker_time, end=marker_time + duration)

    @staticmethod
    def _resolve_event_start(elem, inferred_date, inferred_tz, last_time_offset):
        explicit = _combine_date_and_time(elem.get("Date"), elem.get("Time"))
        if explicit is not None:
            local_explicit = explicit.astimezone(inferred_tz)
            next_date = local_explicit.date()
            next_offset = _parse_duration(elem.get("Time"))
            if next_offset is None:
                next_offset = timedelta(
                    hours=local_explicit.hour,
                    minutes=local_explicit.minute,
                    seconds=local_explicit.second,
                    microseconds=local_explicit.microsecond,
                )
            return explicit, next_date, next_offset

        time_offset = _parse_duration(elem.get("Time"))
        if time_offset is None:
            return None, inferred_date, last_time_offset

        next_date = inferred_date
        if last_time_offset is not None and time_offset < last_time_offset:
            next_date = inferred_date + timedelta(days=1)

        start_time = datetime(next_date.year, next_date.month, next_date.day, tzinfo=inferred_tz) + time_offset
        return start_time, next_date, time_offset

    @staticmethod
    def _merge_intervals(intervals):
        if not intervals:
            return []

        merged = []
        for interval in sorted(intervals, key=lambda item: (item.start, item.end)):
            if not merged:
                merged.append([interval.start, interval.end])
                continue

            prev_start, prev_end = merged[-1]
            if interval.start <= prev_end:
                if interval.end > prev_end:
                    merged[-1][1] = interval.end
            else:
                merged.append([interval.start, interval.end])

        return [Interval(start=start, end=end) for start, end in merged if end > start]

    @staticmethod
    def _build_off_intervals_from_markers(markers):
        if len(markers) < 2:
            return []

        windows = []
        pending_off_start = None
        for marker in sorted(markers, key=lambda item: (item.time, item.order)):
            if marker.is_off:
                pending_off_start = marker.time
                continue

            if pending_off_start is None:
                continue

            if not marker.is_on:
                continue

            if marker.time > pending_off_start:
                windows.append(Interval(start=pending_off_start, end=marker.time))
            pending_off_start = None

        return LogoScheduleTimeline._merge_intervals(windows)

    @staticmethod
    def _normalize_material_ids(values, fallback):
        source = values
        if source is None:
            source = fallback
        if isinstance(source, str):
            source = [source]
        if not isinstance(source, (list, tuple, set)):
            raise ValueError("Material ID list must be a string or a sequence of strings.")

        normalized = []
        seen = set()
        for item in source:
            token = LogoScheduleTimeline._normalize_material_token(item)
            if not token or token in seen:
                continue
            seen.add(token)
            normalized.append(token)

        if not normalized:
            normalized = [LogoScheduleTimeline._normalize_material_token(item) for item in fallback]
            normalized = [item for item in normalized if item]
        if not normalized:
            raise ValueError("Material ID list cannot be empty.")
        return tuple(normalized)

    @staticmethod
    def _normalize_material_token(value):
        return "".join(str(value or "").upper().split())

    @staticmethod
    def _matches_any_pattern(value, patterns):
        for pattern in patterns:
            if "*" in pattern or "?" in pattern or "[" in pattern:
                if fnmatch.fnmatchcase(value, pattern):
                    return True
                continue

            if value == pattern:
                return True
        return False

    @staticmethod
    def _find_interval(target, intervals, starts):
        if not intervals:
            return None

        idx = bisect.bisect_right(starts, target) - 1
        if idx < 0:
            return None

        interval = intervals[idx]
        if interval.start <= target < interval.end:
            return interval
        return None
