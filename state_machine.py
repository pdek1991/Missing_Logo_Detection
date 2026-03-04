import time


class StateMachine:
    def __init__(self, config):
        self.threshold_yellow = float(config.get("threshold_yellow", 0.60))
        self.threshold_red = float(config.get("threshold_red", 0.50))
        if self.threshold_red > self.threshold_yellow:
            self.threshold_red, self.threshold_yellow = self.threshold_yellow, self.threshold_red

        self.warmup_cycles = max(0, int(config.get("warmup_cycles", 2)))
        self.freeze_seconds = max(5.0, float(config.get("freeze_seconds", 20.0)))

        self.state = "WARMUP"
        self.frame_count = 0
        self.consecutive_failures = 0

        self.last_hash = None
        self.freeze_since = None

    def is_verifying(self):
        return False

    def start_verification(self):
        return None

    def evaluate_verification(self):
        return self.state, "N/A"

    def update(self, frame_hash, score, error, verification_step=False):
        """
        Classify one stream sample. The scheduler calls this once per polling cycle.
        """
        del verification_step
        self.frame_count += 1
        display_score = f"{score:.2f}" if score is not None else "N/A"

        if error:
            self.state = self._error_to_state(error)
            self.consecutive_failures += 1
            self.last_hash = frame_hash
            self.freeze_since = None
            return self.state, display_score

        if self._is_frozen(frame_hash):
            self.state = "FROZEN_FRAME"
            self.consecutive_failures += 1
            return self.state, display_score

        if self.frame_count <= self.warmup_cycles:
            self.state = "WARMUP"
            return self.state, display_score

        if score is None:
            self.state = "LOGO MISSING"
            self.consecutive_failures += 1
            return self.state, display_score

        if score >= self.threshold_yellow:
            self.state = "LOGO PRESENT"
            self.consecutive_failures = 0
        elif score >= self.threshold_red:
            self.state = "LOGO MAYBE MISSING"
            self.consecutive_failures += 1
        else:
            self.state = "LOGO MISSING"
            self.consecutive_failures += 1

        return self.state, display_score

    def _is_frozen(self, frame_hash):
        if frame_hash is None:
            self.last_hash = None
            self.freeze_since = None
            return False

        if self.last_hash == frame_hash:
            if self.freeze_since is None:
                self.freeze_since = time.monotonic()
                return False
            return (time.monotonic() - self.freeze_since) >= self.freeze_seconds

        self.last_hash = frame_hash
        self.freeze_since = None
        return False

    @staticmethod
    def _error_to_state(error):
        text = str(error).lower()
        if "no packets" in text:
            return "NO_PACKETS_RECEIVED"
        if "timeout" in text:
            return "STREAM_TIMEOUT"
        if "corrupt" in text:
            return "STREAM_CORRUPT"
        if "decode" in text or "decoder" in text:
            return "STREAM_DECODE_ERROR"
        return "STREAM_DOWN"
