import subprocess

import cv2
import numpy as np


class FFmpegReader:
    def __init__(self, url, timeout_seconds=4.0, scale="640:360", ffmpeg_bin="ffmpeg"):
        self.url = url
        self.timeout_seconds = float(timeout_seconds)
        self.scale = scale
        self.ffmpeg_bin = ffmpeg_bin

    def capture_frame(self, timeout=None):
        """
        Extract one video frame using ffmpeg and decode it into OpenCV format.
        Returns: (frame, error_message)
        """
        timeout = float(timeout or self.timeout_seconds)
        cmd = [
            self.ffmpeg_bin,
            "-hide_banner",
            "-nostdin",
            "-loglevel",
            "error",
            "-fflags",
            "+discardcorrupt",
            "-analyzeduration",
            "1000000",
            "-probesize",
            "1000000",
            "-i",
            self.url,
            "-an",
            "-sn",
            "-dn",
            "-frames:v",
            "1",
            "-vf",
            f"scale={self.scale}",
            "-f",
            "image2pipe",
            "-vcodec",
            "png",
            "pipe:1",
        ]

        process = None
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.DEVNULL,
            )
            stdout, stderr = process.communicate(timeout=timeout)

            if process.returncode != 0:
                return None, self._normalize_error(stderr.decode("utf-8", errors="ignore"))

            if not stdout:
                return None, "no packets received"

            np_arr = np.frombuffer(stdout, np.uint8)
            frame = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
            if frame is None:
                return None, "decoder failure (invalid image)"

            return frame, None

        except subprocess.TimeoutExpired:
            if process is not None:
                process.kill()
                _, stderr = process.communicate()
                msg = stderr.decode("utf-8", errors="ignore") if stderr else ""
                normalized = self._normalize_error(msg)
                if "no packets" in normalized:
                    return None, normalized
            return None, "stream timeout"
        except FileNotFoundError:
            return None, "ffmpeg binary not found"
        except Exception as exc:
            return None, f"reader error: {exc}"

    @staticmethod
    def _normalize_error(raw_error):
        text = (raw_error or "").strip().lower()
        if not text:
            return "decoder failure"
        if "timed out" in text:
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
