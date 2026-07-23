from __future__ import annotations

import bisect
import filecmp
import os
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

from PyQt6.QtCore import QObject, pyqtSignal

from foto_cleaner.content import categorize, jpeg_payload_digest

# Video files match when their sizes are within this fraction of each other.
VIDEO_SIZE_TOLERANCE = 0.01  # 1%


@dataclass
class DupResult:
    target_abs: str      # absolute path of the target file
    target_rel: str      # path relative to the target folder (name if no subdirs)
    dup_abs: str         # absolute path of the confirmed duplicate in search folder
    dup_rel: str         # path relative to the search folder
    size: int            # size of the target file in bytes


class DuplicateFinderWorker(QObject):
    """
    Finds files in `target_folder` that also exist somewhere under
    `search_folder` (recursively).

    Two matching modes:
    - Name mode (default): match by filename first, then confirm with a full
      byte-for-byte comparison.
    - Content mode (ignore_names): filenames are ignored. JPEGs are matched by
      their image payload (metadata ignored); videos are matched by size within
      a 1% tolerance; other file types are not compared. The match_jpeg /
      match_video flags restrict which categories are considered.
    """

    progress = pyqtSignal(int, int, str)     # total, processed, current filename
    status = pyqtSignal(str)
    duplicate_found = pyqtSignal(object)     # DupResult, emitted as each is found
    finished = pyqtSignal(list, dict)        # List[DupResult], stats dict
    error = pyqtSignal(str)

    def __init__(
        self,
        target_folder: str,
        search_folder: str,
        ignore_names: bool = False,
        match_jpeg: bool = True,
        match_video: bool = True,
    ):
        super().__init__()
        self.target_folder = os.path.abspath(target_folder)
        self.search_folder = os.path.abspath(search_folder)
        self.ignore_names = bool(ignore_names)
        self.match_jpeg = bool(match_jpeg)
        self.match_video = bool(match_video)
        self._cancelled = False

    def cancel(self) -> None:
        self._cancelled = True

    def run(self) -> None:
        try:
            self.status.emit("Collecting target files…")
            target_files = self._collect_target_files(self.target_folder)
            total = len(target_files)
            if total == 0:
                self.status.emit("No files found in target folder.")
                self.finished.emit([], self._stats([], total))
                return

            # When the target and search folders are the same, every copy would
            # otherwise list every other copy as its duplicate, so checking all
            # rows would delete the whole set. In that mode we keep the first
            # occurrence of each identical group as the original and only report
            # the later copies.
            same_dir = self.target_folder == self.search_folder

            self.status.emit("Indexing search folder…")
            find_candidates, confirm = self._build_matcher(same_dir)
            if self._cancelled:
                self.status.emit("Cancelled.")
                self.finished.emit([], self._stats([], total))
                return

            self.status.emit(f"Comparing {total} files…")
            results: List[DupResult] = []
            kept: set[str] = set()  # originals to preserve (same-dir mode)
            for processed, tpath in enumerate(target_files, start=1):
                if self._cancelled:
                    self.status.emit("Cancelled.")
                    self.finished.emit([], self._stats([], total))
                    return

                self.progress.emit(total, processed, os.path.basename(tpath))

                comparable, candidates = find_candidates(tpath)
                if not comparable:
                    continue  # file type not compared in this mode

                match = self._pick_match(
                    tpath, candidates, only=kept if same_dir else None, confirm=confirm
                )
                if same_dir and match is None:
                    # First occurrence of its group becomes the kept original.
                    kept.add(os.path.abspath(tpath))
                if match is not None:
                    try:
                        size = os.path.getsize(tpath)
                    except OSError:
                        size = 0
                    result = DupResult(
                        target_abs=tpath,
                        target_rel=os.path.relpath(tpath, self.target_folder),
                        dup_abs=match,
                        dup_rel=os.path.relpath(match, self.search_folder),
                        size=size,
                    )
                    results.append(result)
                    # Target files are processed in sorted order, so streaming
                    # rows keeps the table sorted by target file as it fills.
                    self.duplicate_found.emit(result)

            self.status.emit(
                f"Done. {len(results)} duplicate(s) found in {total} file(s)."
            )
            self.finished.emit(results, self._stats(results, total))

        except Exception as e:  # noqa: BLE001 - surface any error to the UI
            self.error.emit(f"Duplicate finder error: {e}")

    # ---- matcher construction ------------------------------------------

    def _build_matcher(
        self, same_dir: bool
    ) -> Tuple[Callable[[str], Tuple[bool, list]], Callable[[str, str], bool]]:
        """Return (find_candidates, confirm) for the active matching mode.

        find_candidates(target) -> (comparable, [candidate abs paths])
        confirm(target, candidate) -> bool  (final check before accepting)
        """
        if not self.ignore_names:
            name_index = self._index_by_name(self.search_folder, skip_duplicates=same_dir)

            def find_by_name(tpath: str):
                return True, name_index.get(os.path.basename(tpath), ())

            def confirm_bytes(target: str, cand: str) -> bool:
                try:
                    return filecmp.cmp(target, cand, shallow=False)
                except OSError:
                    return False

            return find_by_name, confirm_bytes

        # Content mode: index by JPEG payload and/or video size.
        jpeg_index, video_sorted = self._index_by_content(
            self.search_folder, skip_duplicates=same_dir
        )
        video_sizes = [s for s, _ in video_sorted]

        def find_by_content(tpath: str):
            cat = categorize(tpath)
            if cat == "jpeg" and self.match_jpeg:
                digest = jpeg_payload_digest(tpath)
                if digest is None:
                    return False, ()
                return True, jpeg_index.get(digest, ())
            if cat == "video" and self.match_video:
                try:
                    size = os.path.getsize(tpath)
                except OSError:
                    return False, ()
                lo = bisect.bisect_left(video_sizes, size * (1 - VIDEO_SIZE_TOLERANCE))
                hi = bisect.bisect_right(video_sizes, size * (1 + VIDEO_SIZE_TOLERANCE))
                return True, [p for _, p in video_sorted[lo:hi]]
            return False, ()

        # Candidates already satisfy the matching criterion; no further check.
        def confirm_true(target: str, cand: str) -> bool:
            return True

        return find_by_content, confirm_true

    def _pick_match(
        self,
        target: str,
        candidates,
        only: Optional[set],
        confirm: Callable[[str, str], bool],
    ) -> Optional[str]:
        """First candidate that passes confirm (excluding the file itself).

        If `only` is given, restrict to candidates in that set (already-kept
        originals, used in same-dir mode).
        """
        target_abs = os.path.abspath(target)
        for cand in candidates:
            cand_abs = os.path.abspath(cand)
            if cand_abs == target_abs:
                continue  # same physical file (overlapping folders)
            if only is not None and cand_abs not in only:
                continue
            if confirm(target, cand):
                return cand
        return None

    # ---- indexing ------------------------------------------------------

    def _collect_target_files(self, root: str) -> List[str]:
        """All files under root (skipping duplicates/), sorted by relative path."""
        collected = list(self._walk_files(root, skip_duplicates=True))
        collected.sort(key=lambda p: os.path.relpath(p, root).lower())
        return collected

    def _walk_files(self, root: str, skip_duplicates: bool):
        """Yield absolute file paths under root, optionally skipping duplicates/."""
        duplicates_dir = os.path.join(root, "duplicates")
        for dirpath, dirnames, filenames in os.walk(root):
            if self._cancelled:
                return
            if skip_duplicates:
                dirnames[:] = [
                    d
                    for d in dirnames
                    if os.path.join(dirpath, d) != duplicates_dir and d != "duplicates"
                ]
            for fn in filenames:
                yield os.path.join(dirpath, fn)

    def _index_by_name(
        self, root: str, skip_duplicates: bool = False
    ) -> Dict[str, List[str]]:
        """Map basename -> list of absolute paths under the search folder."""
        index: Dict[str, List[str]] = {}
        for path in self._walk_files(root, skip_duplicates):
            index.setdefault(os.path.basename(path), []).append(path)
        return index

    def _index_by_content(
        self, root: str, skip_duplicates: bool = False
    ) -> Tuple[Dict[str, List[str]], List[Tuple[int, str]]]:
        """Build (jpeg payload-digest index, sorted list of (size, video path))."""
        jpeg_index: Dict[str, List[str]] = {}
        videos: List[Tuple[int, str]] = []
        for path in self._walk_files(root, skip_duplicates):
            if self._cancelled:
                break
            cat = categorize(path)
            if cat == "jpeg" and self.match_jpeg:
                digest = jpeg_payload_digest(path)
                if digest is not None:
                    jpeg_index.setdefault(digest, []).append(path)
            elif cat == "video" and self.match_video:
                try:
                    videos.append((os.path.getsize(path), path))
                except OSError:
                    continue
        for paths in jpeg_index.values():
            paths.sort()
        videos.sort()
        return jpeg_index, videos

    @staticmethod
    def _stats(results: List[DupResult], target_count: int) -> dict:
        return {
            "target_count": target_count,
            "dup_count": len(results),
            "total_size": sum(r.size for r in results),
        }
