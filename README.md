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
- Results table (sorted by target file) with columns:
  - **Target file** — relative path within the target folder (or filename)
  - **Duplicate found in** — directory + filename of the confirmed copy
  - **Remove** — checkbox (checked by default) to select for moving
- Stats line shows target file count, duplicates found, and total duplicate size
- **Move duplicates** → moves checked files into a `duplicates/` folder inside the
  target folder, **preserving the original subfolder structure**
- **Clear** → full reset (results, stats, and selected folders)

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
