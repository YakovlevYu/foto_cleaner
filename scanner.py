from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Iterable, List

from PIL import Image, UnidentifiedImageError
import imagehash

from PyQt6.QtCore import QObject, pyqtSignal


SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tif", ".tiff", ".webp"}


@dataclass(frozen=True)
class ImageItem:
    path: str
    phash: imagehash.ImageHash


class ScannerWorker(QObject):
    """
    Runs scanning in a background thread (move this QObject to a QThread).
    Emits groups of similar image paths at the end.

    Groups are connected-components based on pairwise similarity.
    Similarity uses perceptual hash (phash) + Hamming distance threshold.
    """
    progress = pyqtSignal(int, int, str)     # total, current, current_file
    finished = pyqtSignal(list)              # List[List[str]] groups
    error = pyqtSignal(str)
    status = pyqtSignal(str)

    def __init__(self, root_folder: str, threshold: int = 6, bucket_prefix_hex: int = 4):
        super().__init__()
        self.root_folder = os.path.abspath(root_folder)
        self.threshold = int(threshold)
        self.bucket_prefix_hex = int(bucket_prefix_hex)
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        try:
            self.status.emit("Collecting images…")
            files = list(self._collect_images(self.root_folder))
            total = len(files)
            if total == 0:
                self.finished.emit([])
                return

            self.status.emit(f"Hashing {total} images…")
            items: List[ImageItem] = []
            for i, path in enumerate(files, start=1):
                if self._cancelled:
                    self.status.emit("Cancelled.")
                    self.finished.emit([])
                    return
                self.progress.emit(total, i, os.path.basename(path))
                h = self._compute_phash(path)
                if h is not None:
                    items.append(ImageItem(path=path, phash=h))

            if self._cancelled:
                self.status.emit("Cancelled.")
                self.finished.emit([])
                return

            self.status.emit("Finding similar groups…")
            groups = self._find_similar_groups(items, threshold=self.threshold, bucket_prefix_hex=self.bucket_prefix_hex)
            # Sort groups by size desc then by first filename for stable output
            groups.sort(key=lambda g: (-len(g), g[0].lower() if g else ""))
            self.finished.emit(groups)

        except Exception as e:
            self.error.emit(f"Scanner error: {e}")

    def _collect_images(self, root: str) -> Iterable[str]:
        removed_dir = os.path.join(root, "removed")
        for dirpath, dirnames, filenames in os.walk(root):
            # Skip removed/ entirely
            # Modify dirnames in-place to prevent walk into removed
            dirnames[:] = [d for d in dirnames if os.path.join(dirpath, d) != removed_dir and d != "removed"]

            for fn in filenames:
                ext = os.path.splitext(fn)[1].lower()
                if ext in SUPPORTED_EXTS:
                    yield os.path.join(dirpath, fn)

    def _compute_phash(self, path: str):
        try:
            # Use context manager to close file quickly
            with Image.open(path) as im:
                im = im.convert("RGB")
                return imagehash.phash(im)  # 64-bit by default
        except (UnidentifiedImageError, OSError):
            # Corrupt/unsupported file, skip silently
            return None

    def _find_similar_groups(
        self,
        items: List[ImageItem],
        threshold: int,
        bucket_prefix_hex: int,
    ) -> List[List[str]]:
        """
        Efficient-ish grouping:
        - Bucket by phash hex prefix (first N hex chars)
        - Compare within bucket; union if distance <= threshold
        - Return connected components with size >= 2
        """
        if not items:
            return []

        n = len(items)

        # DSU / Union-Find
        parent = list(range(n))
        rank = [0] * n

        def find(a: int) -> int:
            while parent[a] != a:
                parent[a] = parent[parent[a]]
                a = parent[a]
            return a

        def union(a: int, b: int) -> None:
            ra, rb = find(a), find(b)
            if ra == rb:
                return
            if rank[ra] < rank[rb]:
                parent[ra] = rb
            elif rank[ra] > rank[rb]:
                parent[rb] = ra
            else:
                parent[rb] = ra
                rank[ra] += 1

        # Buckets (banding): multiple keys per image to avoid missing near matches
        buckets: Dict[str, List[int]] = {}
        for idx, it in enumerate(items):
            hx = str(it.phash)  # 16 hex chars for 64-bit hash
            # 4 bands of 4 hex chars each
            bands = (hx[0:4], hx[4:8], hx[8:12], hx[12:16])
            for b in bands:
                buckets.setdefault(b, []).append(idx)
        print(buckets)

        # Compare within each bucket, avoiding duplicate comparisons
        seen_pairs = set()
        for key, idxs in buckets.items():
            if len(idxs) < 2:
                continue
            for a_pos in range(len(idxs) - 1):
                a = idxs[a_pos]
                for b_pos in range(a_pos + 1, len(idxs)):
                    b = idxs[b_pos]
                    pair = (a, b) if a < b else (b, a)
                    if pair in seen_pairs:
                        continue
                    seen_pairs.add(pair)
                    if (items[a].phash - items[b].phash) <= threshold:
                        union(a, b)

        # Collect components
        comps: Dict[int, List[str]] = {}
        for i in range(n):
            r = find(i)
            comps.setdefault(r, []).append(items[i].path)

        groups = [sorted(paths) for paths in comps.values() if len(paths) >= 2]
        return groups
