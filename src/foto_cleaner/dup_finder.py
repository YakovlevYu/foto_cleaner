from __future__ import annotations

import filecmp
import os
from dataclasses import dataclass
from typing import Dict, List

from PyQt6.QtCore import QObject, pyqtSignal


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
    `search_folder` (recursively). Matches by filename first, then confirms
    with a full byte-for-byte content comparison (the "100%" check).
    """

    progress = pyqtSignal(int, int, str)     # total, processed, current filename
    status = pyqtSignal(str)
    duplicate_found = pyqtSignal(object)     # DupResult, emitted as each is found
    finished = pyqtSignal(list, dict)        # List[DupResult], stats dict
    error = pyqtSignal(str)

    def __init__(self, target_folder: str, search_folder: str):
        super().__init__()
        self.target_folder = os.path.abspath(target_folder)
        self.search_folder = os.path.abspath(search_folder)
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

            self.status.emit("Indexing search folder…")
            search_index = self._index_search_folder(self.search_folder)
            if self._cancelled:
                self.status.emit("Cancelled.")
                self.finished.emit([], self._stats([], total))
                return

            self.status.emit(f"Comparing {total} files…")
            results: List[DupResult] = []
            for processed, tpath in enumerate(target_files, start=1):
                if self._cancelled:
                    self.status.emit("Cancelled.")
                    self.finished.emit([], self._stats([], total))
                    return

                self.progress.emit(total, processed, os.path.basename(tpath))

                name = os.path.basename(tpath)
                candidates = search_index.get(name, ())
                match = self._first_content_match(tpath, candidates)
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

    def _collect_target_files(self, root: str) -> List[str]:
        """All files under root, skipping the target's own duplicates/ folder."""
        duplicates_dir = os.path.join(root, "duplicates")
        collected: List[str] = []
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [
                d
                for d in dirnames
                if os.path.join(dirpath, d) != duplicates_dir and d != "duplicates"
            ]
            for fn in filenames:
                collected.append(os.path.join(dirpath, fn))
        collected.sort(key=lambda p: os.path.relpath(p, root).lower())
        return collected

    def _index_search_folder(self, root: str) -> Dict[str, List[str]]:
        """Map basename -> list of absolute paths under the search folder."""
        index: Dict[str, List[str]] = {}
        for dirpath, _dirnames, filenames in os.walk(root):
            if self._cancelled:
                return index
            for fn in filenames:
                index.setdefault(fn, []).append(os.path.join(dirpath, fn))
        return index

    def _first_content_match(self, target: str, candidates) -> str | None:
        """Return the first candidate that is a 100% byte match of target."""
        target_abs = os.path.abspath(target)
        for cand in candidates:
            if os.path.abspath(cand) == target_abs:
                # Same physical file (target folder overlaps search folder).
                continue
            try:
                if filecmp.cmp(target, cand, shallow=False):
                    return cand
            except OSError:
                continue
        return None

    @staticmethod
    def _stats(results: List[DupResult], target_count: int) -> dict:
        return {
            "target_count": target_count,
            "dup_count": len(results),
            "total_size": sum(r.size for r in results),
        }
