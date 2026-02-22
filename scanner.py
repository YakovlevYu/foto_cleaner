from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from PIL import Image, UnidentifiedImageError
import imagehash

from PyQt6.QtCore import QObject, pyqtSignal


SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".tif", ".tiff", ".webp", ".heic"}


@dataclass
class Component:
    root: int
    min_idx: int
    max_idx: int
    members: List[int]  # indices into paths


class ScannerWorker(QObject):
    """
    Incremental scanner:
      - collect + sort paths
      - compute hashes lazily
      - add similarity edges only from i to i+1..i+k
      - DSU forms components
      - emit a component only when it's "closed":
            current_i > component.max_idx + k
        meaning no future node can connect to it (because edges are only within window k)

    After emitting a group, the worker PAUSES until UI calls continue_scan().
    """

    progress = pyqtSignal(int, int, str)     # total_files, hashed_count, current_file
    status = pyqtSignal(str)

    group_found = pyqtSignal(list)           # List[str] paths for the found group
    finished = pyqtSignal()
    error = pyqtSignal(str)

    def __init__(self, root_folder: str, threshold: int = 6, window_k: int = 1):
        super().__init__()
        self.root_folder = os.path.abspath(root_folder)
        self.threshold = int(threshold)
        self.window_k = max(1, int(window_k))

        self._cancelled = False
        self._resume_event = threading.Event()
        self._resume_event.set()  # not paused at start

    def cancel(self) -> None:
        self._cancelled = True
        self._resume_event.set()

    def continue_scan(self) -> None:
        """Called by UI after Skip / Remove selected."""
        self._resume_event.set()

    def run(self) -> None:
        try:
            self.status.emit("Collecting images…")
            paths = self._collect_sorted_images(self.root_folder)
            n = len(paths)
            if n == 0:
                self.status.emit("No images found.")
                self.finished.emit()
                return

            self.status.emit(f"Scanning {n} images (window k={self.window_k})…")

            # Lazy hash storage
            hashes: List[Optional[imagehash.ImageHash]] = [None] * n
            hashed_count = 0

            # DSU arrays
            parent = list(range(n))
            rank = [0] * n

            # Component metadata stored by root
            comps: Dict[int, Component] = {
                i: Component(root=i, min_idx=i, max_idx=i, members=[i]) for i in range(n)
            }

            emitted_roots = set()  # components already emitted

            def find(a: int) -> int:
                while parent[a] != a:
                    parent[a] = parent[parent[a]]
                    a = parent[a]
                return a

            def union(a: int, b: int) -> int:
                ra, rb = find(a), find(b)
                if ra == rb:
                    return ra

                # union by rank
                if rank[ra] < rank[rb]:
                    ra, rb = rb, ra
                parent[rb] = ra
                if rank[ra] == rank[rb]:
                    rank[ra] += 1

                # merge component metadata into ra
                ca = comps[ra]
                cb = comps[rb]
                ca.min_idx = min(ca.min_idx, cb.min_idx)
                ca.max_idx = max(ca.max_idx, cb.max_idx)
                ca.members.extend(cb.members)
                comps[ra] = ca
                del comps[rb]

                # If rb was emitted, treat merged as emitted as well (defensive).
                if rb in emitted_roots:
                    emitted_roots.add(ra)

                return ra

            def ensure_hash(idx: int) -> Optional[imagehash.ImageHash]:
                nonlocal hashed_count
                if hashes[idx] is not None:
                    return hashes[idx]
                if self._cancelled:
                    return None

                p = paths[idx]
                self.progress.emit(n, hashed_count + 1, os.path.basename(p))
                h = self._compute_phash(p)
                hashes[idx] = h
                if h is not None:
                    hashed_count += 1
                return h

            def is_closed(comp: Component, current_i: int) -> bool:
                # Closed means no future index can connect to comp due to window constraint
                return current_i > comp.max_idx + self.window_k

            # Main scan pointer
            i = 0
            while i < n:
                if self._cancelled:
                    self.status.emit("Cancelled.")
                    self.finished.emit()
                    return

                hi = ensure_hash(i)
                if hi is None:
                    i += 1
                    continue

                # Compare to following window
                j_max = min(n - 1, i + self.window_k)
                for j in range(i + 1, j_max + 1):
                    if self._cancelled:
                        self.status.emit("Cancelled.")
                        self.finished.emit()
                        return

                    hj = ensure_hash(j)
                    if hj is None:
                        continue

                    d = hi - hj
                    if d <= self.threshold:
                        union(i, j)

                i += 1

                # After advancing i, emit earliest closed group if any (one at a time)
                closed_candidates: List[Tuple[int, Component]] = []
                for r, c in comps.items():
                    if r in emitted_roots:
                        continue
                    if len(c.members) >= 2 and is_closed(c, i):
                        closed_candidates.append((c.min_idx, c))

                if closed_candidates:
                    closed_candidates.sort(key=lambda t: t[0])  # earliest min_idx first
                    comp = closed_candidates[0][1]

                    # Emit this group (paths sorted by filename order already)
                    group_paths = [paths[idx] for idx in sorted(comp.members)]
                    emitted_roots.add(comp.root)

                    # Pause
                    self._resume_event.clear()
                    self.group_found.emit(group_paths)
                    self.status.emit("Paused: waiting for user action…")
                    self._resume_event.wait()  # wait until UI continues or cancel
                    if self._cancelled:
                        self.status.emit("Cancelled.")
                        self.finished.emit()
                        return

            # End of scan: also emit any remaining groups (they're closed now)
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

            self.status.emit("Done.")
            self.finished.emit()

        except Exception as e:
            self.error.emit(f"Scanner error: {e}")

    def _collect_sorted_images(self, root: str) -> List[str]:
        removed_dir = os.path.join(root, "removed")

        collected: List[str] = []
        for dirpath, dirnames, filenames in os.walk(root):
            # Skip removed/
            dirnames[:] = [d for d in dirnames if os.path.join(dirpath, d) != removed_dir and d != "removed"]

            for fn in filenames:
                ext = os.path.splitext(fn)[1].lower()
                if ext in SUPPORTED_EXTS:
                    collected.append(os.path.join(dirpath, fn))

        # Sort by relative path for stable locality within folders
        collected.sort(key=lambda p: os.path.relpath(p, root).lower())
        return collected

    def _compute_phash(self, path: str) -> Optional[imagehash.ImageHash]:
        try:
            with Image.open(path) as im:
                im = im.convert("RGB")
                return imagehash.phash(im)
        except (UnidentifiedImageError, OSError):
            return None
