import os

import cv2
import numpy as np


class LogoDetector:
    def __init__(self, template_path, roi=None):
        self.template = None
        self.roi = roi or {"x": 500, "y": 0, "width": 140, "height": 80}
        self._load_template(template_path)

    def _load_template(self, path):
        """
        Load template once during startup to avoid per-frame disk IO.
        """
        if os.path.exists(path):
            img = cv2.imread(path, cv2.IMREAD_GRAYSCALE)
            if img is not None:
                width = max(1, int(self.roi.get("width", 1)))
                height = max(1, int(self.roi.get("height", 1)))
                self.template = cv2.resize(img, (width, height), interpolation=cv2.INTER_AREA)

        if self.template is None:
            width = max(1, int(self.roi.get("width", 1)))
            height = max(1, int(self.roi.get("height", 1)))
            self.template = np.zeros((height, width), dtype=np.uint8)

    def process_frame(self, frame):
        """
        Return normalized template matching confidence [0.0, 1.0].
        """
        if frame is None or frame.size == 0:
            return None

        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        gray = cv2.normalize(gray, None, 0, 255, cv2.NORM_MINMAX)

        h_img, w_img = gray.shape[:2]
        x = max(0, min(int(self.roi.get("x", 0)), w_img - 1))
        y = max(0, min(int(self.roi.get("y", 0)), h_img - 1))
        w = max(1, int(self.roi.get("width", 1)))
        h = max(1, int(self.roi.get("height", 1)))
        x2 = max(0, min(x + w, w_img))
        y2 = max(0, min(y + h, h_img))

        if x2 <= x or y2 <= y:
            return None

        roi = gray[y:y2, x:x2]
        target_h, target_w = self.template.shape[:2]
        if roi.shape[0] != target_h or roi.shape[1] != target_w:
            roi = cv2.resize(roi, (target_w, target_h), interpolation=cv2.INTER_AREA)

        res = cv2.matchTemplate(roi, self.template, cv2.TM_CCOEFF_NORMED)
        _, max_val, _, _ = cv2.minMaxLoc(res)
        return max(0.0, min(1.0, float(max_val)))
