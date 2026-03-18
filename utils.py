import cv2
import numpy as np


def compute_perceptual_hash(frame):
    """
    Compute a compact dHash-like fingerprint for frozen-frame detection.
    Returns None for invalid frames.
    """
    if frame is None or frame.size == 0:
        return None

    gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
    small = cv2.resize(gray, (9, 8), interpolation=cv2.INTER_AREA)
    diff = small[:, 1:] > small[:, :-1]
    bits = ''.join('1' if x else '0' for x in diff.flatten())
    return hex(int(bits, 2))[2:].rjust(16, '0')
