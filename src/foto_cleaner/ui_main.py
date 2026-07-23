from __future__ import annotations

import sys

from PyQt6.QtGui import QCloseEvent
from PyQt6.QtWidgets import (
    QApplication,
    QButtonGroup,
    QHBoxLayout,
    QMainWindow,
    QPushButton,
    QStackedWidget,
    QVBoxLayout,
    QWidget,
)

from foto_cleaner.ui_similar import SimilarPhotosWidget
from foto_cleaner.ui_duplicates import DuplicatesWidget


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Foto Cleaner")

        central = QWidget()
        self.setCentralWidget(central)
        layout = QVBoxLayout(central)

        # Mode toggle bar.
        mode_bar = QHBoxLayout()
        layout.addLayout(mode_bar)

        self.similar_btn = QPushButton("Similar photos")
        self.duplicates_btn = QPushButton("Duplicates")
        for btn in (self.similar_btn, self.duplicates_btn):
            btn.setCheckable(True)

        self._mode_group = QButtonGroup(self)
        self._mode_group.setExclusive(True)
        self._mode_group.addButton(self.similar_btn, 0)
        self._mode_group.addButton(self.duplicates_btn, 1)
        mode_bar.addWidget(self.similar_btn)
        mode_bar.addWidget(self.duplicates_btn)
        mode_bar.addStretch(1)

        # Pages.
        self.stack = QStackedWidget()
        layout.addWidget(self.stack, stretch=1)

        self.similar_page = SimilarPhotosWidget()
        self.duplicates_page = DuplicatesWidget()
        self.stack.addWidget(self.similar_page)      # index 0
        self.stack.addWidget(self.duplicates_page)   # index 1

        self._mode_group.idClicked.connect(self._switch_mode)

        self.similar_btn.setChecked(True)
        self.stack.setCurrentIndex(0)

    def _switch_mode(self, index: int) -> None:
        # Clearing local vars on switch is a product requirement.
        self.similar_page.clear_state()
        self.duplicates_page.clear_state()
        self.stack.setCurrentIndex(index)

    def closeEvent(self, event: QCloseEvent) -> None:
        self.similar_page.shutdown()
        self.duplicates_page.shutdown()
        event.accept()


def main() -> None:
    app = QApplication(sys.argv)
    w = MainWindow()
    w.resize(1100, 700)
    w.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
