import os

import cv2
import numpy as np


class LogoDetector:
    def __init__(self, template_path, roi=None):
        self.template = None
        self.template_loaded = False
        self.roi = roi or {"x": 500, "y": 0, "width": 140, "height": 80}
        self._load_template(template_path)

    def _load_template(self, path):
        """
        Load and normalize template once during startup.
        """
        if os.path.exists(path):
            img = cv2.imread(path, cv2.IMREAD_GRAYSCALE)
            if img is not None:
                width = max(1, int(self.roi.get("width", 1)))
                height = max(1, int(self.roi.get("height", 1)))
                self.template = cv2.resize(img, (width, height), interpolation=cv2.INTER_AREA)
                self.template = cv2.normalize(self.template, None, 0, 255, cv2.NORM_MINMAX)
                self.template_loaded = True

        if self.template is None:
            width = max(1, int(self.roi.get("width", 1)))
            height = max(1, int(self.roi.get("height", 1)))
            self.template = np.zeros((height, width), dtype=np.uint8)
            self.template_loaded = False

    def _extract_roi_gray(self, frame, roi_region=None):
        if frame is None or frame.size == 0:
            return None

        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        gray = cv2.normalize(gray, None, 0, 255, cv2.NORM_MINMAX)

        target_roi = roi_region or self.roi
        h_img, w_img = gray.shape[:2]
        x = max(0, min(int(target_roi.get("x", 0)), w_img - 1))
        y = max(0, min(int(target_roi.get("y", 0)), h_img - 1))
        w = max(1, int(target_roi.get("width", 1)))
        h = max(1, int(target_roi.get("height", 1)))
        x2 = max(0, min(x + w, w_img))
        y2 = max(0, min(y + h, h_img))

        if x2 <= x or y2 <= y:
            return None

        roi = gray[y:y2, x:x2]
        target_h, target_w = self.template.shape[:2]
        if roi.shape[0] != target_h or roi.shape[1] != target_w:
            roi = cv2.resize(roi, (target_w, target_h), interpolation=cv2.INTER_AREA)
        return roi

    def process_frame(self, frame, roi_region=None):
        """
        Return normalized template matching confidence [0.0, 1.0].
        """
        roi = self._extract_roi_gray(frame, roi_region=roi_region)
        if roi is None:
            return None

        if not self.template_loaded:
            return 0.0

        res = cv2.matchTemplate(roi, self.template, cv2.TM_CCOEFF_NORMED)
        _, max_val, _, _ = cv2.minMaxLoc(res)
        return max(0.0, min(1.0, float(max_val)))

    def edge_verify(self, frame, roi_region=None):
        del frame, roi_region
        return None

    def orb_verify(self, frame, roi_region=None, ratio_threshold=0.75):
        del frame, roi_region, ratio_threshold
        return None
