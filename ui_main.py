from __future__ import annotations

import os
import sys
from typing import List, Optional, Set

from PyQt6.QtCore import Qt, QThread, QSize
from PyQt6.QtGui import QAction, QIcon
from PyQt6.QtWidgets import (
    QApplication,
    QFileDialog,
    QHBoxLayout,
    QLabel,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QProgressBar,
    QSpinBox,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from scanner import ScannerWorker
from thumbnailer import Thumbnailer
from ops import open_in_viewer, move_to_removed


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Similar Photo Finder")

        # State
        self.root_folder: Optional[str] = None
        self.groups: List[List[str]] = []
        self.group_index: int = 0
        self.thumbnailer = Thumbnailer(size=200)

        self._scan_thread: Optional[QThread] = None
        self._scanner: Optional[ScannerWorker] = None

        # UI
        self._build_ui()
        self._set_idle_state()

    def _build_ui(self) -> None:
        # Toolbar
        tb = QToolBar("Main")
        self.addToolBar(tb)

        open_action = QAction("Open Folder…", self)
        open_action.triggered.connect(self.pick_folder)
        tb.addAction(open_action)

        tb.addSeparator()

        tb.addWidget(QLabel("Similarity threshold:"))
        self.threshold_spin = QSpinBox()
        self.threshold_spin.setRange(0, 20)
        self.threshold_spin.setValue(5)
        self.threshold_spin.setToolTip("Max Hamming distance for phash. Lower = stricter, fewer matches.")
        tb.addWidget(self.threshold_spin)

        tb.addSeparator()

        self.status_label = QLabel("Select a folder to start.")
        tb.addWidget(self.status_label)

        # Central
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        # Progress
        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setTextVisible(True)
        main_layout.addWidget(self.progress)

        # Group label
        self.group_label = QLabel("")
        self.group_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        main_layout.addWidget(self.group_label)

        # Thumbnails list
        self.listw = QListWidget()
        self.listw.setViewMode(QListWidget.ViewMode.IconMode)
        self.listw.setResizeMode(QListWidget.ResizeMode.Adjust)
        self.listw.setMovement(QListWidget.Movement.Static)
        self.listw.setIconSize(QSize(200, 200))
        # self.listw.setIconSize(self.thumbnailer.get_pixmap.__self__.size if hasattr(self.thumbnailer.get_pixmap, "__self__") else None)
        self.listw.setSpacing(10)
        self.listw.setSelectionMode(QListWidget.SelectionMode.NoSelection)  # we use checkboxes
        self.listw.itemDoubleClicked.connect(self._open_item)
        main_layout.addWidget(self.listw, stretch=1)

        # Buttons
        btn_row = QHBoxLayout()
        main_layout.addLayout(btn_row)

        self.remove_btn = QPushButton("Remove selected")
        self.remove_btn.clicked.connect(self.remove_selected)
        btn_row.addWidget(self.remove_btn)

        self.skip_btn = QPushButton("Skip")
        self.skip_btn.clicked.connect(self.skip_group)
        btn_row.addWidget(self.skip_btn)

        self.scan_btn = QPushButton("Scan")
        self.scan_btn.clicked.connect(self.start_scan)
        btn_row.addWidget(self.scan_btn)

    def _set_idle_state(self) -> None:
        self.progress.setValue(0)
        self.group_label.setText("")
        self.listw.clear()
        self.remove_btn.setEnabled(False)
        self.skip_btn.setEnabled(False)
        self.scan_btn.setEnabled(self.root_folder is not None)

    def pick_folder(self) -> None:
        folder = QFileDialog.getExistingDirectory(self, "Select folder to scan")
        if not folder:
            return
        self.root_folder = os.path.abspath(folder)
        self.status_label.setText(f"Selected: {self.root_folder}")
        self._set_idle_state()

    def start_scan(self) -> None:
        if not self.root_folder:
            QMessageBox.information(self, "No folder", "Please select a folder first.")
            return

        # Prevent multiple scans
        if self._scan_thread is not None:
            QMessageBox.information(self, "Scan in progress", "A scan is already running.")
            return

        self.groups = []
        self.group_index = 0
        self.listw.clear()

        threshold = int(self.threshold_spin.value())

        self.status_label.setText("Starting scan…")
        self.progress.setRange(0, 0)  # indeterminate until first progress arrives
        self.scan_btn.setEnabled(False)
        self.remove_btn.setEnabled(False)
        self.skip_btn.setEnabled(False)

        self._scan_thread = QThread()
        self._scanner = ScannerWorker(root_folder=self.root_folder, threshold=threshold, bucket_prefix_hex=4)
        self._scanner.moveToThread(self._scan_thread)

        self._scan_thread.started.connect(self._scanner.run)
        self._scanner.progress.connect(self._on_progress)
        self._scanner.status.connect(self._on_status)
        self._scanner.finished.connect(self._on_finished)
        self._scanner.error.connect(self._on_error)

        # Clean-up
        self._scanner.finished.connect(self._scan_thread.quit)
        self._scanner.error.connect(self._scan_thread.quit)
        self._scan_thread.finished.connect(self._cleanup_scan)

        self._scan_thread.start()

    def _cleanup_scan(self) -> None:
        if self._scanner is not None:
            self._scanner.deleteLater()
        if self._scan_thread is not None:
            self._scan_thread.deleteLater()
        self._scanner = None
        self._scan_thread = None

    def _on_status(self, text: str) -> None:
        self.status_label.setText(text)

    def _on_progress(self, total: int, current: int, filename: str) -> None:
        # Switch to determinate if needed
        if self.progress.maximum() == 0:
            self.progress.setRange(0, total)
        self.progress.setValue(current)
        self.status_label.setText(f"Hashing: {current}/{total} — {filename}")

    def _on_error(self, msg: str) -> None:
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.scan_btn.setEnabled(True)
        QMessageBox.critical(self, "Error", msg)
        self.status_label.setText("Error.")

    def _on_finished(self, groups: List[List[str]]) -> None:
        self.progress.setRange(0, 100)
        self.progress.setValue(100)
        self.groups = groups
        self.group_index = 0

        if not self.groups:
            self.status_label.setText("No similar groups found.")
            self._set_idle_state()
            return

        self.status_label.setText(f"Found {len(self.groups)} similar group(s).")
        self._show_current_group()
        self.scan_btn.setEnabled(True)

    def _show_current_group(self) -> None:
        self.listw.clear()

        if not self.root_folder:
            return

        if self.group_index >= len(self.groups):
            self.group_label.setText("")
            self.status_label.setText("Done. No more groups.")
            self.remove_btn.setEnabled(False)
            self.skip_btn.setEnabled(False)
            return

        group = self.groups[self.group_index]
        self.group_label.setText(f"Group {self.group_index + 1} / {len(self.groups)} — {len(group)} image(s)")

        for path in group:
            item = QListWidgetItem()
            item.setText(os.path.basename(path))
            item.setToolTip(path)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(Qt.CheckState.Unchecked)

            pix = self.thumbnailer.get_pixmap(path)
            if pix is not None:
                item.setIcon(QIcon(pix))

            # Store full path
            item.setData(Qt.ItemDataRole.UserRole, path)
            self.listw.addItem(item)

        self.remove_btn.setEnabled(True)
        self.skip_btn.setEnabled(True)

    def _open_item(self, item: QListWidgetItem) -> None:
        path = item.data(Qt.ItemDataRole.UserRole)
        if isinstance(path, str) and os.path.exists(path):
            open_in_viewer(path)

    def _get_checked_paths(self) -> List[str]:
        checked: List[str] = []
        for i in range(self.listw.count()):
            it = self.listw.item(i)
            if it.checkState() == Qt.CheckState.Checked:
                p = it.data(Qt.ItemDataRole.UserRole)
                if isinstance(p, str):
                    checked.append(p)
        return checked

    def remove_selected(self) -> None:
        if not self.root_folder:
            return

        checked = self._get_checked_paths()
        if not checked:
            QMessageBox.information(self, "Nothing selected", "Select one or more images to remove.")
            return

        try:
            move_to_removed(checked, self.root_folder)
        except Exception as e:
            QMessageBox.critical(self, "Move failed", f"Failed to move selected files:\n{e}")
            return

        # Remove moved images from current group listing and underlying group list
        current_group = set(self.groups[self.group_index])
        remaining = [p for p in self.groups[self.group_index] if p not in set(checked)]
        self.groups[self.group_index] = remaining

        # If group shrinks below 2, skip automatically
        if len(remaining) < 2:
            self.group_index += 1
            self._show_current_group()
            return

        # Otherwise refresh the current group view
        self._show_current_group()

    def skip_group(self) -> None:
        self.group_index += 1
        self._show_current_group()


def main() -> None:
    app = QApplication(sys.argv)
    w = MainWindow()
    w.resize(1100, 700)
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
