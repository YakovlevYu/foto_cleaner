"""Content-based comparison helpers for name-agnostic duplicate matching.

Two files downloaded from different places can differ by a few bytes even when
they show the same picture/video, because metadata (EXIF, container atoms, …)
differs. These helpers compare the meaningful payload instead of raw bytes:

- JPEG: hash only the image payload (frame headers + entropy-coded scan),
  skipping APPn/COM metadata segments, so metadata-only differences match.
- Video: matched by size within a tolerance (see dup_finder), so no content
  digest is needed here.
- Everything else: not comparable in this mode.
"""

from __future__ import annotations

import hashlib
import os
from typing import Optional

JPEG_EXTS = {".jpg", ".jpeg", ".jpe", ".jfif"}
VIDEO_EXTS = {
    ".mp4", ".mov", ".m4v", ".avi", ".mkv", ".wmv", ".flv", ".webm",
    ".mpg", ".mpeg", ".3gp", ".mts", ".m2ts", ".ts", ".ogv",
}

# JPEG marker bytes (the byte after 0xFF).
_APP_MARKERS = set(range(0xE0, 0xF0))   # APP0..APP15
_COM_MARKER = 0xFE                       # comment
_SKIP_MARKERS = _APP_MARKERS | {_COM_MARKER}
_SOI = 0xD8
_EOI = 0xD9
_SOS = 0xDA
_TEM = 0x01


def categorize(path: str) -> Optional[str]:
    """Return 'jpeg', 'video', or None based on the file extension."""
    ext = os.path.splitext(path)[1].lower()
    if ext in JPEG_EXTS:
        return "jpeg"
    if ext in VIDEO_EXTS:
        return "video"
    return None


def jpeg_payload_digest(path: str) -> Optional[str]:
    """Hash a JPEG's image payload, ignoring metadata (APPn/COM) segments.

    Returns a hex digest, or None if the file is not a parseable JPEG. Two JPEGs
    that differ only in metadata (EXIF, JFIF, ICC, thumbnails, comments) produce
    the same digest; different pixel data produces different digests.
    """
    try:
        with open(path, "rb") as f:
            data = f.read()
    except OSError:
        return None

    n = len(data)
    if n < 2 or data[0] != 0xFF or data[1] != _SOI:
        return None  # not a JPEG (missing SOI)

    h = hashlib.sha256()
    i = 2
    try:
        while i < n:
            # Every marker starts with one or more 0xFF fill bytes.
            if data[i] != 0xFF:
                return None
            while i < n and data[i] == 0xFF:
                i += 1
            if i >= n:
                break
            marker = data[i]
            i += 1

            if marker == _EOI:
                break
            if marker == _TEM or 0xD0 <= marker <= 0xD7:
                # Standalone markers (TEM, RSTn) carry no length/payload.
                continue

            if i + 1 >= n:
                break
            seg_len = (data[i] << 8) | data[i + 1]
            if seg_len < 2:
                return None
            segment = data[i:i + seg_len]  # includes the 2 length bytes
            i += seg_len

            if marker == _SOS:
                # Entropy-coded scan data follows the SOS header until the next
                # real marker (0xFF not followed by 0x00 or an RSTn marker).
                start = i
                while i < n:
                    if data[i] == 0xFF and i + 1 < n:
                        nb = data[i + 1]
                        if nb == 0x00 or 0xD0 <= nb <= 0xD7:
                            i += 2
                            continue
                        break
                    i += 1
                h.update(b"\xda")
                h.update(segment)
                h.update(data[start:i])
                continue

            if marker in _SKIP_MARKERS:
                continue

            h.update(bytes([marker]))
            h.update(segment)
    except IndexError:
        return None

    return h.hexdigest()
