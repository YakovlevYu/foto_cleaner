from __future__ import annotations

import os
import shutil
import subprocess
from typing import Iterable, List


def open_in_viewer(path: str) -> None:
    """
    Open in the system default viewer (Ubuntu: xdg-open).
    Non-blocking.
    """
    subprocess.Popen(["xdg-open", path], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def ensure_removed_dir(root_folder: str) -> str:
    removed = os.path.join(root_folder, "removed")
    os.makedirs(removed, exist_ok=True)
    return removed


def _unique_destination(dest_dir: str, filename: str) -> str:
    """
    If filename exists, append _(<n>) before extension.
    """
    base, ext = os.path.splitext(filename)
    candidate = os.path.join(dest_dir, filename)
    if not os.path.exists(candidate):
        return candidate

    n = 1
    while True:
        new_name = f"{base}_({n}){ext}"
        candidate = os.path.join(dest_dir, new_name)
        if not os.path.exists(candidate):
            return candidate
        n += 1


def move_to_removed(paths: Iterable[str], root_folder: str) -> List[str]:
    """
    Move selected files into root_folder/removed. Creates removed on demand.
    Returns list of destination paths.
    """
    removed_dir = ensure_removed_dir(root_folder)
    moved_to: List[str] = []
    for p in paths:
        if not os.path.isfile(p):
            continue
        filename = os.path.basename(p)
        dst = _unique_destination(removed_dir, filename)
        shutil.move(p, dst)
        moved_to.append(dst)
    return moved_to