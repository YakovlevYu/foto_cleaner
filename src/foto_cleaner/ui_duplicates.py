from __future__ import annotations

import os
from typing import List, Optional

from PyQt6.QtCore import Qt, QThread
from PyQt6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QFileDialog,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from foto_cleaner.dup_finder import DuplicateFinderWorker, DupResult
from foto_cleaner.ops import move_duplicates


class _CaseInsensitiveItem(QTableWidgetItem):
    """Table item that sorts case-insensitively (so 'IMG' and 'img' interleave)."""

    def __lt__(self, other: QTableWidgetItem) -> bool:  # type: ignore[override]
        return self.text().lower() < other.text().lower()


class _NumericItem(QTableWidgetItem):
    """Table item that sorts by a numeric value stored in UserRole."""

    def __lt__(self, other: QTableWidgetItem) -> bool:  # type: ignore[override]
        a = self.data(Qt.ItemDataRole.UserRole)
        b = other.data(Qt.ItemDataRole.UserRole)
        try:
            return float(a) < float(b)
        except (TypeError, ValueError):
            return super().__lt__(other)


def _human_size(num: int) -> str:
    size = float(num)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024.0 or unit == "TB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} TB"


class DuplicatesWidget(QWidget):
    """Find files in a target folder that also exist under a search folder."""

    COL_TARGET = 0
    COL_DUP = 1
    COL_SIZE = 2
    COL_REMOVE = 3

    def __init__(self, parent: Optional[QWidget] = None):
        super().__init__(parent)

        self.target_folder: Optional[str] = None
        self.search_folder: Optional[str] = None
        self.results: List[DupResult] = []

        self._thread: Optional[QThread] = None
        self._worker: Optional[DuplicateFinderWorker] = None
        self._closing = False
        self._active_search = False

        self._build_ui()
        self._update_button_states()

    def _build_ui(self) -> None:
        main_layout = QVBoxLayout(self)

        # Folder selection row.
        controls = QHBoxLayout()
        main_layout.addLayout(controls)

        target_btn = QPushButton("Target folder…")
        target_btn.clicked.connect(self.pick_target_folder)
        controls.addWidget(target_btn)
        self.target_label = QLabel("(not set)")
        controls.addWidget(self.target_label, stretch=1)

        search_btn = QPushButton("Search folder…")
        search_btn.clicked.connect(self.pick_search_folder)
        controls.addWidget(search_btn)
        self.search_label = QLabel("(not set)")
        controls.addWidget(self.search_label, stretch=1)

        # Matching mode.
        self.ignore_names_cb = QCheckBox("Compare content without name (match by size, then content)")
        self.ignore_names_cb.setToolTip(
            "When checked, filenames are ignored: files of equal size are compared "
            "byte-for-byte to find duplicates."
        )
        main_layout.addWidget(self.ignore_names_cb)

        # Stats line.
        self.stats_label = QLabel("No search run yet.")
        main_layout.addWidget(self.stats_label)

        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setTextVisible(True)
        main_layout.addWidget(self.progress)

        self.status_label = QLabel("Select target and search folders, then press Search.")
        main_layout.addWidget(self.status_label)

        # Results table.
        self.table = QTableWidget(0, 4)
        self.table.setHorizontalHeaderLabels(
            ["Target file", "Duplicate found in", "Size", "Remove"]
        )
        self.table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.table.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        header = self.table.horizontalHeader()
        header.setSectionResizeMode(self.COL_TARGET, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(self.COL_DUP, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(self.COL_SIZE, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(self.COL_REMOVE, QHeaderView.ResizeMode.ResizeToContents)
        self.table.setSortingEnabled(True)
        main_layout.addWidget(self.table, stretch=1)

        # Bottom button row: Search / Move / Clear.
        btn_row = QHBoxLayout()
        main_layout.addLayout(btn_row)

        self.search_action_btn = QPushButton("Search")
        self.search_action_btn.clicked.connect(self.start_search)
        btn_row.addWidget(self.search_action_btn)

        self.move_btn = QPushButton("Move duplicates")
        self.move_btn.clicked.connect(self.move_selected)
        btn_row.addWidget(self.move_btn)

        self.clear_btn = QPushButton("Clear")
        self.clear_btn.clicked.connect(self.clear_state)
        btn_row.addWidget(self.clear_btn)

    # ---- state helpers -------------------------------------------------

    def _search_is_running(self) -> bool:
        return self._thread is not None and self._thread.isRunning()

    def _update_button_states(self) -> None:
        running = self._search_is_running()
        can_search = (
            self.target_folder is not None
            and self.search_folder is not None
            and not running
        )
        self.search_action_btn.setEnabled(can_search)
        self.move_btn.setEnabled(not running and self.table.rowCount() > 0)
        self.clear_btn.setEnabled(not running)

    def clear_state(self) -> None:
        """Full reset: stop any search, clear table, stats, and folder inputs."""
        self._active_search = False
        if self._search_is_running() and self._worker is not None:
            self._worker.cancel()
            if self._thread is not None:
                self._thread.quit()
                self._thread.wait(1500)
                if self._thread is not None and not self._thread.isRunning():
                    self._cleanup_search()

        self.target_folder = None
        self.search_folder = None
        self.results = []
        self.target_label.setText("(not set)")
        self.search_label.setText("(not set)")
        self.ignore_names_cb.setChecked(False)
        self.stats_label.setText("No search run yet.")
        self.status_label.setText("Select target and search folders, then press Search.")
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        self.table.setSortingEnabled(True)
        self._update_button_states()

    def shutdown(self) -> None:
        """Gracefully stop an in-progress search (called on app close)."""
        if self._closing:
            return
        thread = self._thread
        worker = self._worker
        if thread is not None and thread.isRunning():
            self._closing = True
            if worker is not None:
                worker.cancel()
            thread.quit()
            thread.wait(1500)
            if self._thread is not None and not self._thread.isRunning():
                self._cleanup_search()

    # ---- folder selection ----------------------------------------------

    def pick_target_folder(self) -> None:
        folder = QFileDialog.getExistingDirectory(self, "Select target folder")
        if not folder:
            return
        self.target_folder = os.path.abspath(folder)
        self.target_label.setText(self.target_folder)
        self._update_button_states()

    def pick_search_folder(self) -> None:
        folder = QFileDialog.getExistingDirectory(self, "Select search folder")
        if not folder:
            return
        self.search_folder = os.path.abspath(folder)
        self.search_label.setText(self.search_folder)
        self._update_button_states()

    # ---- search lifecycle ----------------------------------------------

    def start_search(self) -> None:
        if not self.target_folder or not self.search_folder:
            QMessageBox.information(self, "Folders required", "Select both a target and a search folder.")
            return
        if self._search_is_running():
            return
        if self._thread is not None:
            self._cleanup_search()

        self.results = []
        self._active_search = True
        # Sorting off while streaming rows in (re-enabled when the search finishes).
        self.table.setSortingEnabled(False)
        self.table.setRowCount(0)
        self.stats_label.setText("Duplicates found: 0   |   Total size: 0 B")
        self.status_label.setText("Starting search…")
        self.progress.setRange(0, 0)  # indeterminate until comparison starts

        self._thread = QThread()
        self._worker = DuplicateFinderWorker(
            self.target_folder,
            self.search_folder,
            ignore_names=self.ignore_names_cb.isChecked(),
        )
        self._worker.moveToThread(self._thread)

        self._thread.started.connect(self._worker.run)
        self._worker.progress.connect(self._on_progress)
        self._worker.status.connect(self._on_status)
        self._worker.duplicate_found.connect(self._on_duplicate_found)
        self._worker.finished.connect(self._on_finished)
        self._worker.error.connect(self._on_error)

        self._worker.finished.connect(self._thread.quit)
        self._worker.error.connect(self._thread.quit)
        self._thread.finished.connect(self._cleanup_search)

        self._thread.start()
        self._update_button_states()

    def _cleanup_search(self) -> None:
        if self._worker is not None:
            self._worker.deleteLater()
        if self._thread is not None:
            self._thread.deleteLater()
        self._worker = None
        self._thread = None
        self._closing = False
        self._update_button_states()

    def _on_status(self, text: str) -> None:
        self.status_label.setText(text)

    def _on_progress(self, total: int, current: int, filename: str) -> None:
        if self.progress.maximum() == 0:
            self.progress.setRange(0, total)
        self.progress.setValue(current)
        self.status_label.setText(f"Comparing: {current}/{total} — {filename}")

    def _on_error(self, msg: str) -> None:
        self._active_search = False
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        QMessageBox.critical(self, "Error", msg)
        self.status_label.setText("Error.")
        self._update_button_states()

    def _on_duplicate_found(self, r: DupResult) -> None:
        if not self._active_search:
            return
        self.results.append(r)
        self._append_row(r)
        total_size = sum(x.size for x in self.results)
        self.stats_label.setText(
            f"Duplicates found: {len(self.results)}   |   "
            f"Total size: {_human_size(total_size)}"
        )

    def _on_finished(self, results: List[DupResult], stats: dict) -> None:
        # Ignore a finished signal that arrives after a reset/clear.
        if not self._active_search:
            return
        self._active_search = False
        self.progress.setRange(0, 100)
        self.progress.setValue(100)
        # Rows were streamed in (sorting was off); re-enable interactive sorting
        # now, keeping the deterministic target-file order.
        self.table.setSortingEnabled(True)
        self.table.sortItems(self.COL_TARGET, Qt.SortOrder.AscendingOrder)
        # Rows were streamed in as duplicates were found; just finalize stats.
        self.stats_label.setText(
            f"Target files: {stats.get('target_count', 0)}   |   "
            f"Duplicates found: {stats.get('dup_count', 0)}   |   "
            f"Total size: {_human_size(stats.get('total_size', 0))}"
        )
        self.status_label.setText("Done.")
        self._update_button_states()

    def _set_row(self, row: int, r: DupResult) -> None:
        target_item = _CaseInsensitiveItem(r.target_rel)
        target_item.setToolTip(r.target_abs)
        target_item.setData(Qt.ItemDataRole.UserRole, r.target_abs)
        self.table.setItem(row, self.COL_TARGET, target_item)

        # Path of the duplicate relative to the search folder.
        dup_item = _CaseInsensitiveItem(r.dup_rel)
        dup_item.setToolTip(r.dup_abs)
        self.table.setItem(row, self.COL_DUP, dup_item)

        size_item = _NumericItem(_human_size(r.size))
        size_item.setData(Qt.ItemDataRole.UserRole, r.size)
        size_item.setTextAlignment(
            Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter
        )
        self.table.setItem(row, self.COL_SIZE, size_item)

        remove_item = QTableWidgetItem()
        remove_item.setFlags(
            Qt.ItemFlag.ItemIsUserCheckable | Qt.ItemFlag.ItemIsEnabled
        )
        remove_item.setCheckState(Qt.CheckState.Checked)
        remove_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        self.table.setItem(row, self.COL_REMOVE, remove_item)

    def _populate_table(self, results: List[DupResult]) -> None:
        # Disable live sorting while filling, then restore a deterministic order.
        self.table.setSortingEnabled(False)
        self.table.setRowCount(len(results))
        for row, r in enumerate(results):
            self._set_row(row, r)
        self.table.setSortingEnabled(True)
        self.table.sortItems(self.COL_TARGET, Qt.SortOrder.AscendingOrder)

    def _append_row(self, r: DupResult) -> None:
        # Sorting stays disabled during streaming; target files arrive in sorted
        # order, so a plain append keeps the table sorted by target file.
        row = self.table.rowCount()
        self.table.insertRow(row)
        self._set_row(row, r)

    # ---- move ----------------------------------------------------------

    def _checked_target_paths(self) -> List[str]:
        checked: List[str] = []
        for row in range(self.table.rowCount()):
            remove_item = self.table.item(row, self.COL_REMOVE)
            target_item = self.table.item(row, self.COL_TARGET)
            if remove_item is None or target_item is None:
                continue
            if remove_item.checkState() == Qt.CheckState.Checked:
                path = target_item.data(Qt.ItemDataRole.UserRole)
                if isinstance(path, str):
                    checked.append(path)
        return checked

    def move_selected(self) -> None:
        if not self.target_folder:
            return
        checked = self._checked_target_paths()
        if not checked:
            QMessageBox.information(self, "Nothing selected", "Check one or more rows to move.")
            return

        try:
            move_duplicates(checked, self.target_folder)
        except Exception as e:
            QMessageBox.critical(self, "Move failed", f"Failed to move duplicates:\n{e}")
            return

        moved = set(os.path.abspath(p) for p in checked)
        remaining = [r for r in self.results if os.path.abspath(r.target_abs) not in moved]
        self.results = remaining
        self._populate_table(remaining)
        total_size = sum(r.size for r in remaining)
        self.stats_label.setText(
            f"Moved {len(checked)} file(s) to '{os.path.join(self.target_folder, 'duplicates')}'.   |   "
            f"Remaining duplicates: {len(remaining)}   |   "
            f"Total size: {_human_size(total_size)}"
        )
        self.status_label.setText("Move complete.")
        self._update_button_states()
