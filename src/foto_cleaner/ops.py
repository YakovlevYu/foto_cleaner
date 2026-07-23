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


def move_duplicates(paths: Iterable[str], target_folder: str) -> List[str]:
    """
    Move selected files into target_folder/duplicates, preserving the
    subdirectory structure each file had relative to target_folder.

    Example: target_folder/sub/a.jpg -> target_folder/duplicates/sub/a.jpg

    Creates the duplicates folder (and any needed subfolders) on demand.
    Returns the list of destination paths.
    """
    target_folder = os.path.abspath(target_folder)
    duplicates_root = os.path.join(target_folder, "duplicates")
    moved_to: List[str] = []
    for p in paths:
        p = os.path.abspath(p)
        if not os.path.isfile(p):
            continue

        rel = os.path.relpath(p, target_folder)
        # Guard against paths outside target_folder: fall back to basename.
        if rel.startswith(os.pardir + os.sep) or rel == os.pardir:
            rel = os.path.basename(p)

        dest_dir = os.path.join(duplicates_root, os.path.dirname(rel))
        os.makedirs(dest_dir, exist_ok=True)
        dst = _unique_destination(dest_dir, os.path.basename(p))
        shutil.move(p, dst)
        moved_to.append(dst)
    return moved_to