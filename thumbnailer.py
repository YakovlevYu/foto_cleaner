from __future__ import annotations

import os
from typing import Dict, Optional, Tuple

from PIL import Image, ImageOps
from PyQt6.QtGui import QImage, QPixmap


class Thumbnailer:
    """
    Creates thumbnails for images with a simple in-memory cache.
    """
    def __init__(self, size: int = 200):
        self.size = int(size)
        self._cache: Dict[Tuple[str, int], QPixmap] = {}

    def get_pixmap(self, path: str) -> Optional[QPixmap]:
        key = (path, self.size)
        if key in self._cache:
            return self._cache[key]

        try:
            with Image.open(path) as im:
                im = ImageOps.exif_transpose(im)  # respect EXIF rotation
                im = im.convert("RGB")
                im.thumbnail((self.size, self.size))
                qimg = self._pil_to_qimage(im)
                pix = QPixmap.fromImage(qimg)
                self._cache[key] = pix
                return pix
        except Exception:
            return None

    def _pil_to_qimage(self, im: Image.Image) -> QImage:
        # RGB PIL -> QImage
        w, h = im.size
        data = im.tobytes("raw", "RGB")
        qimg = QImage(data, w, h, 3 * w, QImage.Format.Format_RGB888)
        # Make a deep copy so the buffer isn't freed when `data` goes out of scope
        return qimg.copy()
