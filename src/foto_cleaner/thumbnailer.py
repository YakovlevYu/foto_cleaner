from __future__ import annotations

from typing import Dict, Optional, Tuple

from PIL import Image, ImageOps
from PyQt6.QtGui import QImage, QPixmap

# NEW: HEIC/HEIF Pillow plugin
try:
    from pillow_heif import register_heif_opener
    register_heif_opener()
except Exception:
    pass


class Thumbnailer:
    def __init__(self, size: int = 200):
        self.size = int(size)
        self._cache: Dict[Tuple[str, int], QPixmap] = {}

    def get_pixmap(self, path: str) -> Optional[QPixmap]:
        key = (path, self.size)
        if key in self._cache:
            return self._cache[key]

        try:
            with Image.open(path) as im:
                im = ImageOps.exif_transpose(im)
                im = im.convert("RGB")
                im.thumbnail((self.size, self.size))
                qimg = self._pil_to_qimage(im)
                pix = QPixmap.fromImage(qimg)
                self._cache[key] = pix
                return pix
        except Exception:
            return None

    def _pil_to_qimage(self, im: Image.Image) -> QImage:
        w, h = im.size
        data = im.tobytes("raw", "RGB")
        qimg = QImage(data, w, h, 3 * w, QImage.Format.Format_RGB888)
        return qimg.copy()
