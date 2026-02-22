from __future__ import annotations

import os
import threading
from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from PIL import Image, ImageOps, UnidentifiedImageError
import imagehash

# NEW: HEIC/HEIF Pillow plugin
try:
    from pillow_heif import register_heif_opener
    register_heif_opener()  # enables PIL.Image.open() for .heic/.heif
    _HEIF_ENABLED = True
except Exception:
    _HEIF_ENABLED = False

from PyQt6.QtCore import QObject, pyqtSignal


SUPPORTED_EXTS = {
    ".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tif", ".tiff", ".webp",
    ".heic", ".heif",  # NEW
}


@dataclass
class Component:
    root: int
    min_idx: int
    max_idx: int
    members: List[int]


class ScannerWorker(QObject):
    progress = pyqtSignal(int, int, str)     # total_files, processed_count, current_file
    status = pyqtSignal(str)

    group_found = pyqtSignal(list)           # List[str]
    finished = pyqtSignal()
    error = pyqtSignal(str)

    def __init__(self, root_folder: str, threshold: int = 6, window_k: int = 1):
        super().__init__()
        self.root_folder = os.path.abspath(root_folder)
        self.threshold = int(threshold)
        self.window_k = max(1, int(window_k))

        self._cancelled = False
        self._resume_event = threading.Event()
        self._resume_event.set()

    def cancel(self) -> None:
        self._cancelled = True
        self._resume_event.set()

    def continue_scan(self) -> None:
        self._resume_event.set()

    def run(self) -> None:
        try:
            if not _HEIF_ENABLED:
                # Not fatal: JPG/PNG etc still work. But we warn once.
                self.status.emit("Note: HEIC support not active (install pillow-heif + libheif).")

            self.status.emit("Collecting images…")
            paths = self._collect_sorted_images(self.root_folder)
            n = len(paths)
            if n == 0:
                self.status.emit("No images found.")
                self.finished.emit()
                return

            self.status.emit(f"Scanning {n} images (window k={self.window_k})…")
            self.status.emit("Hashing images in parallel (fast mode)…")

            hashes, processed_count, hashed_count, unreadable_count = self._compute_hashes_parallel(paths)
            if self._cancelled:
                self.status.emit("Cancelled.")
                self.finished.emit()
                return

            parent = list(range(n))
            rank = [0] * n
            comps: Dict[int, Component] = {
                i: Component(root=i, min_idx=i, max_idx=i, members=[i]) for i in range(n)
            }
            emitted_roots = set()

            def find(a: int) -> int:
                while parent[a] != a:
                    parent[a] = parent[parent[a]]
                    a = parent[a]
                return a

            def union(a: int, b: int) -> int:
                ra, rb = find(a), find(b)
                if ra == rb:
                    return ra
                if rank[ra] < rank[rb]:
                    ra, rb = rb, ra
                parent[rb] = ra
                if rank[ra] == rank[rb]:
                    rank[ra] += 1

                ca = comps[ra]
                cb = comps[rb]
                ca.min_idx = min(ca.min_idx, cb.min_idx)
                ca.max_idx = max(ca.max_idx, cb.max_idx)
                ca.members.extend(cb.members)
                comps[ra] = ca
                del comps[rb]
                if rb in emitted_roots:
                    emitted_roots.add(ra)
                return ra

            def is_closed(comp: Component, current_i: int) -> bool:
                return current_i > comp.max_idx + self.window_k

            i = 0
            while i < n:
                if self._cancelled:
                    self.status.emit("Cancelled.")
                    self.finished.emit()
                    return

                hi = hashes[i]
                if hi is None:
                    i += 1
                    continue

                j_max = min(n - 1, i + self.window_k)
                for j in range(i + 1, j_max + 1):
                    hj = hashes[j]
                    if hj is None:
                        continue
                    if (hi - hj) <= self.threshold:
                        union(i, j)

                i += 1

                closed_candidates: List[Tuple[int, Component]] = []
                for r, c in comps.items():
                    if r in emitted_roots:
                        continue
                    if len(c.members) >= 2 and is_closed(c, i):
                        closed_candidates.append((c.min_idx, c))

                if closed_candidates:
                    closed_candidates.sort(key=lambda t: t[0])
                    comp = closed_candidates[0][1]
                    group_paths = [paths[idx] for idx in sorted(comp.members)]
                    emitted_roots.add(comp.root)

                    self._resume_event.clear()
                    self.group_found.emit(group_paths)
                    self.status.emit("Paused: waiting for user action…")
                    self._resume_event.wait()
                    if self._cancelled:
                        self.status.emit("Cancelled.")
                        self.finished.emit()
                        return

            remaining = []
            for r, c in comps.items():
                if r in emitted_roots:
                    continue
                if len(c.members) >= 2:
                    remaining.append((c.min_idx, c))
            remaining.sort(key=lambda t: t[0])

            for _, comp in remaining:
                if self._cancelled:
                    self.status.emit("Cancelled.")
                    self.finished.emit()
                    return
                group_paths = [paths[idx] for idx in sorted(comp.members)]
                emitted_roots.add(comp.root)

                self._resume_event.clear()
                self.group_found.emit(group_paths)
                self.status.emit("Paused: waiting for user action…")
                self._resume_event.wait()
                if self._cancelled:
                    self.status.emit("Cancelled.")
                    self.finished.emit()
                    return

            if unreadable_count:
                self.status.emit(
                    f"Done. Processed {processed_count}/{n} files; "
                    f"{hashed_count} hashed, {unreadable_count} unreadable."
                )
            else:
                self.status.emit("Done.")
            self.finished.emit()

        except Exception as e:
            self.error.emit(f"Scanner error: {e}")

    def _collect_sorted_images(self, root: str) -> List[str]:
        removed_dir = os.path.join(root, "removed")
        collected: List[str] = []
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if os.path.join(dirpath, d) != removed_dir and d != "removed"]
            for fn in filenames:
                ext = os.path.splitext(fn)[1].lower()
                if ext in SUPPORTED_EXTS:
                    collected.append(os.path.join(dirpath, fn))
        collected.sort(key=lambda p: os.path.relpath(p, root).lower())
        return collected

    def _compute_phash(self, path: str) -> Optional[imagehash.ImageHash]:
        # Speed-first / high-recall mode: small dHash is faster than pHash and
        # intentionally catches more candidate matches at the cost of precision.
        try:
            with Image.open(path) as im:
                im = ImageOps.exif_transpose(im)
                im.thumbnail((512, 512))
                im = im.convert("RGB")
                return imagehash.dhash(im, hash_size=8)
        except (UnidentifiedImageError, OSError):
            return None

    def _compute_hashes_parallel(
        self,
        paths: List[str],
    ) -> Tuple[List[Optional[imagehash.ImageHash]], int, int, int]:
        total = len(paths)
        hashes: List[Optional[imagehash.ImageHash]] = [None] * total
        workers = min(32, max(1, (os.cpu_count() or 4)))

        processed = 0
        hashed = 0
        unreadable = 0

        pool = ThreadPoolExecutor(max_workers=workers)
        shutdown_called = False
        future_to_idx = {pool.submit(self._compute_phash, p): i for i, p in enumerate(paths)}

        try:
            for future in as_completed(future_to_idx):
                if self._cancelled:
                    pool.shutdown(wait=False, cancel_futures=True)
                    shutdown_called = True
                    return hashes, processed, hashed, unreadable

                idx = future_to_idx[future]
                try:
                    h = future.result()
                except CancelledError:
                    continue
                except Exception:
                    h = None

                hashes[idx] = h

                processed += 1
                if h is None:
                    unreadable += 1
                else:
                    hashed += 1

                self.progress.emit(total, processed, os.path.basename(paths[idx]))
        finally:
            if not shutdown_called:
                pool.shutdown(wait=True)

        return hashes, processed, hashed, unreadable
