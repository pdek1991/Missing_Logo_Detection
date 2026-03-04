import cv2
import numpy as np

def compute_perceptual_hash(frame):
    """
    Compute a simple difference hash (dHash) for an image.
    Used for freeze frame detection.
    """
    if frame is None:
        return None
        
    # Resize to 9x8 to compute differences between adjacent columns
    resized = cv2.resize(frame, (9, 8), interpolation=cv2.INTER_AREA)
    # Convert to grayscale
    gray = cv2.cvtColor(resized, cv2.COLOR_BGR2GRAY)
    # Compare each pixel with its right neighbor
    diff = gray[:, 1:] > gray[:, :-1]
    
    # Convert boolean array to a single integer hash
    hash_value = sum([2 ** i for (i, v) in enumerate(diff.flatten()) if v])
    return hash_value
