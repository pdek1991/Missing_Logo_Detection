import subprocess
import threading
import time
from collections import deque

import numpy as np


class FFmpegReader:
    """
    Persistent FFmpeg frame reader.
    One FFmpeg process per stream with watchdog-based self-healing.
    """

    def __init__(self, url, timeout_seconds=4.0, scale="640:360", ffmpeg_bin="ffmpeg", output_fps=1.0):
        self.url = url
        self.timeout_seconds = float(timeout_seconds)
        self.scale = scale
        self.ffmpeg_bin = ffmpeg_bin
        self.output_fps = max(0.1, float(output_fps))

        self.width, self.height = self._parse_scale(scale)
        self.frame_bytes = self.width * self.height * 3

        self._lock = threading.Lock()
        self._running = False
        self._worker_thread = None
        self._process = None
        self._frames = deque(maxlen=3)
        self._last_error = "reader not started"
        self._last_frame_time = 0.0
        self._restart_backoff = 0.5

    def start(self):
        with self._lock:
            if self._running:
                return
            self._running = True
            self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True, name="ffmpeg-reader")
            self._worker_thread.start()

    def stop(self):
        with self._lock:
            self._running = False
        self._terminate_process()
        if self._worker_thread is not None and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=2.0)

    def capture_frame(self, timeout=None):
        """
        Return the latest in-memory frame from background FFmpeg worker.
        """
        timeout = max(0.2, float(timeout or self.timeout_seconds))
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            with self._lock:
                if self._frames:
                    ts, frame = self._frames[-1]
                    if (time.monotonic() - ts) <= max(2.5, timeout * 1.25):
                        return frame.copy(), None
                last_error = self._last_error
            time.sleep(0.02)

        with self._lock:
            if not self._running:
                return None, "reader stopped"
            return None, self._normalize_error(last_error)

    def _worker_loop(self):
        while self._is_running():
            process = self._spawn_process()
            if process is None:
                self._sleep_backoff()
                continue

            with self._lock:
                self._process = process
                self._last_error = None
                self._last_frame_time = 0.0
                self._frames.clear()

            start_time = time.monotonic()
            reader_thread = threading.Thread(
                target=self._stdout_reader_loop,
                args=(process,),
                daemon=True,
                name="ffmpeg-reader-stdout",
            )
            reader_thread.start()

            watchdog_timeout = max(4.0, self.timeout_seconds * 2.5)
            forced_restart = False

            while self._is_running():
                if process.poll() is not None:
                    break

                now = time.monotonic()
                with self._lock:
                    last_frame = self._last_frame_time

                if last_frame > 0.0:
                    if (now - last_frame) > watchdog_timeout:
                        forced_restart = True
                        with self._lock:
                            self._last_error = "stream timeout"
                        break
                elif (now - start_time) > watchdog_timeout:
                    forced_restart = True
                    with self._lock:
                        self._last_error = "stream timeout"
                    break

                time.sleep(0.2)

            self._terminate_process()
            if reader_thread.is_alive():
                reader_thread.join(timeout=1.0)

            self._consume_stderr(process)

            if not self._is_running():
                break

            if forced_restart:
                self._sleep_backoff()
            else:
                self._sleep_backoff()

    def _stdout_reader_loop(self, process):
        while self._is_running():
            if process.poll() is not None:
                break

            frame_bytes = self._read_exact(process.stdout, self.frame_bytes)
            if frame_bytes is None:
                break

            frame = np.frombuffer(frame_bytes, dtype=np.uint8)
            if frame.size != self.frame_bytes:
                continue

            frame = frame.reshape((self.height, self.width, 3)).copy()
            now = time.monotonic()
            with self._lock:
                self._frames.append((now, frame))
                self._last_frame_time = now
                self._last_error = None
                self._restart_backoff = 0.5

    def _consume_stderr(self, process):
        if process is None or process.stderr is None:
            return

        stderr_text = ""
        try:
            raw = process.stderr.read()
            if raw:
                stderr_text = raw.decode("utf-8", errors="ignore")
        except Exception:
            stderr_text = ""

        normalized = self._normalize_error(stderr_text or "")
        with self._lock:
            if self._last_error in (None, ""):
                if self._last_frame_time <= 0.0:
                    self._last_error = normalized or "stream unavailable"

    @staticmethod
    def _read_exact(stream, size):
        buf = bytearray()
        while len(buf) < size:
            chunk = stream.read(size - len(buf))
            if not chunk:
                return None
            buf.extend(chunk)
        return bytes(buf)

    def _spawn_process(self):
        rw_timeout_us = int(max(1.0, self.timeout_seconds) * 1_000_000)
        vf_expr = f"fps={self.output_fps},scale={self.width}:{self.height}"
        cmd = [
            self.ffmpeg_bin,
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-fflags",
            "+discardcorrupt+nobuffer",
            "-flags",
            "low_delay",
            "-threads",
            "1",
            "-rw_timeout",
            str(rw_timeout_us),
            "-analyzeduration",
            "1000000",
            "-probesize",
            "1000000",
            "-i",
            self.url,
            "-an",
            "-sn",
            "-dn",
            "-vf",
            vf_expr,
            "-pix_fmt",
            "bgr24",
            "-f",
            "rawvideo",
            "pipe:1",
        ]

        creationflags = 0
        if hasattr(subprocess, "CREATE_NO_WINDOW"):
            creationflags = subprocess.CREATE_NO_WINDOW

        try:
            return subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
                bufsize=self.frame_bytes,
                creationflags=creationflags,
            )
        except FileNotFoundError:
            with self._lock:
                self._last_error = "ffmpeg binary not found"
            return None
        except Exception as exc:
            with self._lock:
                self._last_error = f"reader error: {exc}"
            return None

    def _terminate_process(self):
        process = None
        with self._lock:
            process = self._process
            self._process = None

        if process is None:
            return

        try:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=1.0)
        except Exception:
            try:
                process.kill()
                process.wait(timeout=1.0)
            except Exception:
                pass

    def _sleep_backoff(self):
        delay = min(4.0, max(0.1, self._restart_backoff))
        time.sleep(delay)
        with self._lock:
            self._restart_backoff = min(4.0, self._restart_backoff * 2.0)

    def _is_running(self):
        with self._lock:
            return self._running

    @staticmethod
    def _parse_scale(scale):
        try:
            left, right = str(scale).split(":", 1)
            width = max(1, int(left))
            height = max(1, int(right))
            return width, height
        except Exception:
            return 640, 360

    @staticmethod
    def _normalize_error(raw_error):
        text = (raw_error or "").strip().lower()
        if not text:
            return "stream timeout"
        if "timed out" in text or "timeout" in text:
            return "stream timeout"
        if "invalid data" in text or "error while decoding" in text:
            return "decoder failure"
        if "connection refused" in text or "no route" in text:
            return "stream unavailable"
        if "input/output error" in text:
            return "stream io error"
        if "no such file" in text:
            return "stream unavailable"
        if "non-existing pps" in text or "corrupt" in text:
            return "stream corrupt"
        return f"decoder failure: {text}"
