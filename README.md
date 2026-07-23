# Foto Cleaner

A PyQt6 desktop app for Ubuntu/Linux with two modes, switchable via the buttons
at the top of the window. Switching modes clears the current mode's state.

## Mode 1 — Similar photos

Scans a folder for visually similar photos, shows thumbnail previews, opens images
in the default viewer, and moves selected duplicates into a `removed/` folder.

- Recursive folder scan (skips `removed/`)
- Detects visually similar images using perceptual hashing (dHash)
- Sort-by-name scanning with configurable neighbor window `k`
- Shows groups one by one and pauses for user decision:
  - **Remove selected** → moves selected files to `removed/`
  - **Skip** → continues scanning
- Double-click a thumbnail to open it via `xdg-open`
- Supports HEIC/HEIF via `pillow-heif` + `libheif`

## Mode 2 — Duplicates

Finds files in a **target folder** that also exist somewhere under a **search
folder** (recursively). Matching is done by filename first, then confirmed with a
full byte-for-byte content comparison (a true 100% match).

- Pick a target folder and a search folder, then press **Search**
- Considers **all** files (not just images)
- **Compare content without name** checkbox: when checked, filenames are ignored
  and matching is type-aware (so renamed copies and copies that differ only in
  metadata are detected):
  - **JPEG** files are matched by their image payload — EXIF/JFIF/ICC/comment
    metadata is ignored, so the same photo downloaded twice still matches even
    when the file sizes differ by a few bytes.
  - **Video** files are matched by size within a **1%** tolerance.
  - Other file types are not compared in this mode.
  - **Only JPEG** / **Only videos** checkboxes restrict comparison to that
    category (leave both unchecked to compare both).
  - When unchecked, matching is by filename first, then a full byte-for-byte check.
- **Double-click** a row to open the target file (or the duplicate, if you
  double-click that column) in the default Ubuntu app
- Results stream into the table as each duplicate is confirmed
- Results table (sorted by target file) with columns:
  - **Target file** — relative path within the target folder (or filename)
  - **Duplicate found in** — path of the confirmed copy relative to the search folder
  - **Size** — size of the file (sorts numerically)
  - **Remove** — checkbox (checked by default) to select for moving
- Stats line shows target file count, duplicates found, and total duplicate size
- **Move duplicates** → moves checked files into a `duplicates/` folder inside the
  target folder, **preserving the original subfolder structure**
- **Clear** → full reset (results, stats, and selected folders)
- **Same-folder safety**: when the target and search folders are the same, the
  first copy of each identical set is kept as the original and only the later
  copies are listed, so you can never remove every copy of a file

## Install (Ubuntu)

### System deps (HEIC)
```bash
sudo apt-get update
sudo apt-get install -y libheif1 libheif-dev
```

### Python environment
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
```

### Install the app
```bash
pip install .
```

### Run
```bash
foto-cleaner
```
