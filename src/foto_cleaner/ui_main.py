from __future__ import annotations

import os
import sys
from typing import List, Optional

from PyQt6.QtCore import Qt, QThread, QSize
from PyQt6.QtGui import QAction, QCloseEvent, QIcon
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

from foto_cleaner.scanner import ScannerWorker
from foto_cleaner.thumbnailer import Thumbnailer
from foto_cleaner.ops import open_in_viewer, move_to_removed


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Similar Photo Finder")

        self.root_folder: Optional[str] = None
        self.current_group: List[str] = []
        self.thumbnailer = Thumbnailer(size=200)

        self._scan_thread: Optional[QThread] = None
        self._scanner: Optional[ScannerWorker] = None
        self._closing = False

        self._build_ui()
        self._set_idle_state()

    def _build_ui(self) -> None:
        tb = QToolBar("Main")
        self.addToolBar(tb)

        open_action = QAction("Open Folder…", self)
        open_action.triggered.connect(self.pick_folder)
        tb.addAction(open_action)

        tb.addSeparator()

        tb.addWidget(QLabel("Similarity threshold:"))
        self.threshold_spin = QSpinBox()
        self.threshold_spin.setRange(0, 20)
        self.threshold_spin.setValue(6)
        self.threshold_spin.setToolTip("Max Hamming distance for pHash. Lower = stricter.")
        tb.addWidget(self.threshold_spin)

        tb.addSeparator()

        tb.addWidget(QLabel("Scan window k:"))
        self.window_spin = QSpinBox()
        self.window_spin.setRange(1, 50)
        self.window_spin.setValue(1)
        self.window_spin.setToolTip("Compare each file to the next k files (sorted by name).")
        tb.addWidget(self.window_spin)

        tb.addSeparator()

        self.status_label = QLabel("Select a folder to start.")
        tb.addWidget(self.status_label)

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)

        self.progress = QProgressBar()
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        self.progress.setTextVisible(True)
        main_layout.addWidget(self.progress)

        self.group_label = QLabel("")
        self.group_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        main_layout.addWidget(self.group_label)

        self.listw = QListWidget()
        self.listw.setViewMode(QListWidget.ViewMode.IconMode)
        self.listw.setResizeMode(QListWidget.ResizeMode.Adjust)
        self.listw.setMovement(QListWidget.Movement.Static)
        self.listw.setIconSize(QSize(200, 200))
        self.listw.setSpacing(10)
        self.listw.setSelectionMode(QListWidget.SelectionMode.NoSelection)
        self.listw.itemDoubleClicked.connect(self._open_item)
        main_layout.addWidget(self.listw, stretch=1)

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
        self.current_group = []
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

        if self._scan_thread is not None:
            QMessageBox.information(self, "Scan in progress", "A scan is already running.")
            return

        self.listw.clear()
        self.current_group = []
        self.group_label.setText("")
        self.status_label.setText("Starting scan…")

        threshold = int(self.threshold_spin.value())
        window_k = int(self.window_spin.value())

        self.progress.setRange(0, 0)  # indeterminate until hashing signals start
        self.scan_btn.setEnabled(False)
        self.remove_btn.setEnabled(False)
        self.skip_btn.setEnabled(False)

        self._scan_thread = QThread()
        self._scanner = ScannerWorker(
            root_folder=self.root_folder,
            threshold=threshold,
            window_k=window_k,
        )
        self._scanner.moveToThread(self._scan_thread)

        self._scan_thread.started.connect(self._scanner.run)
        self._scanner.progress.connect(self._on_progress)
        self._scanner.status.connect(self._on_status)
        self._scanner.group_found.connect(self._on_group_found)
        self._scanner.finished.connect(self._on_finished)
        self._scanner.error.connect(self._on_error)

        # Cleanup
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
        self._closing = False
        self.scan_btn.setEnabled(True)

    def closeEvent(self, event: QCloseEvent) -> None:
        # Guard repeated close events while cancellation is already being processed.
        if self._closing:
            event.accept()
            return

        scan_thread = self._scan_thread
        scanner = self._scanner
        is_active = scan_thread is not None and scan_thread.isRunning()

        if is_active:
            self._closing = True
            self.status_label.setText("Stopping scan…")

            if scanner is not None:
                scanner.cancel()

            scan_thread.quit()
            scan_thread.wait(1500)

            # If the thread is already done but the finished signal/event-loop cleanup
            # has not run yet, ensure we still clear references safely.
            if self._scan_thread is not None and not self._scan_thread.isRunning():
                self._cleanup_scan()

        event.accept()

    def _on_status(self, text: str) -> None:
        self.status_label.setText(text)

    def _on_progress(self, total: int, current: int, filename: str) -> None:
        # Switch to determinate
        if self.progress.maximum() == 0:
            self.progress.setRange(0, total)
        self.progress.setValue(current)
        self.status_label.setText(f"Scanning: {current}/{total} — {filename}")

    def _on_error(self, msg: str) -> None:
        self.progress.setRange(0, 100)
        self.progress.setValue(0)
        QMessageBox.critical(self, "Error", msg)
        self.status_label.setText("Error.")
        self._set_idle_state()

    def _on_finished(self) -> None:
        self.progress.setRange(0, 100)
        self.progress.setValue(100)
        self.status_label.setText("Done.")
        self.remove_btn.setEnabled(False)
        self.skip_btn.setEnabled(False)
        # keep thumbnails shown if last group was displayed

    def _on_group_found(self, group_paths: List[str]) -> None:
        # Display group and enable actions; scanner is paused internally until we call continue_scan()
        self.current_group = group_paths
        self._render_group(group_paths)

        self.remove_btn.setEnabled(True)
        self.skip_btn.setEnabled(True)

    def _render_group(self, group: List[str]) -> None:
        self.listw.clear()
        if not group:
            self.group_label.setText("")
            return

        self.group_label.setText(f"Found similar group: {len(group)} image(s)")

        for path in group:
            item = QListWidgetItem()
            item.setText(os.path.basename(path))
            item.setToolTip(path)
            item.setFlags(item.flags() | Qt.ItemFlag.ItemIsUserCheckable)
            item.setCheckState(Qt.CheckState.Unchecked)

            pix = self.thumbnailer.get_pixmap(path)
            if pix is not None:
                item.setIcon(QIcon(pix))

            item.setData(Qt.ItemDataRole.UserRole, path)
            self.listw.addItem(item)

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
        if not self.root_folder or not self._scanner:
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

        # After action: clear UI group and resume scanning
        self.listw.clear()
        self.group_label.setText("")
        self.remove_btn.setEnabled(False)
        self.skip_btn.setEnabled(False)

        self._scanner.continue_scan()

    def skip_group(self) -> None:
        if not self._scanner:
            return

        self.listw.clear()
        self.group_label.setText("")
        self.remove_btn.setEnabled(False)
        self.skip_btn.setEnabled(False)

        self._scanner.continue_scan()


def main() -> None:
    app = QApplication(sys.argv)
    w = MainWindow()
    w.resize(1100, 700)
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
