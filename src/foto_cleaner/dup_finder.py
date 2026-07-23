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

    def __init__(
        self,
        target_folder: str,
        search_folder: str,
        ignore_names: bool = False,
    ):
        super().__init__()
        self.target_folder = os.path.abspath(target_folder)
        self.search_folder = os.path.abspath(search_folder)
        # When True, match by file size first, then confirm by content,
        # ignoring filenames entirely.
        self.ignore_names = bool(ignore_names)
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
            search_index = self._index_search_folder(
                self.search_folder,
                by_size=self.ignore_names,
                skip_duplicates=same_dir,
            )
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

                # Candidate lookup: by size (names ignored) or by filename.
                if self.ignore_names:
                    try:
                        key = os.path.getsize(tpath)
                    except OSError:
                        continue
                else:
                    key = os.path.basename(tpath)
                candidates = search_index.get(key, ())
                if same_dir:
                    # Only match against an already-kept earlier original; the
                    # first file of each identical group becomes that original.
                    match = self._first_content_match(tpath, candidates, only=kept)
                    if match is None:
                        kept.add(os.path.abspath(tpath))
                else:
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

    def _index_search_folder(
        self, root: str, by_size: bool = False, skip_duplicates: bool = False
    ) -> Dict:
        """Index absolute paths under the search folder.

        The key is the file size when by_size is set (name-agnostic matching),
        otherwise the basename. When skip_duplicates is set (same-dir mode), the
        target's own duplicates/ folder is excluded so previously moved copies
        are not treated as originals.
        """
        duplicates_dir = os.path.join(root, "duplicates")
        index: Dict = {}
        for dirpath, dirnames, filenames in os.walk(root):
            if self._cancelled:
                return index
            if skip_duplicates:
                dirnames[:] = [
                    d
                    for d in dirnames
                    if os.path.join(dirpath, d) != duplicates_dir and d != "duplicates"
                ]
            for fn in filenames:
                path = os.path.join(dirpath, fn)
                if by_size:
                    try:
                        key = os.path.getsize(path)
                    except OSError:
                        continue
                else:
                    key = fn
                index.setdefault(key, []).append(path)
        return index

    def _first_content_match(self, target: str, candidates, only=None) -> str | None:
        """Return the first candidate that is a 100% byte match of target.

        If `only` is given, restrict matching to candidates whose absolute path
        is in that set (used to match against already-kept originals).
        """
        target_abs = os.path.abspath(target)
        for cand in candidates:
            cand_abs = os.path.abspath(cand)
            if cand_abs == target_abs:
                # Same physical file (target folder overlaps search folder).
                continue
            if only is not None and cand_abs not in only:
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
