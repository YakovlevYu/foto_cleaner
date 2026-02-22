# Similar Photo Finder

A PyQt6 desktop app for Ubuntu/Linux that scans a folder for visually similar photos, shows thumbnail previews, opens images in the default viewer, and moves selected duplicates into a `removed/` folder.

## Features
- Recursive folder scan (skips `removed/`)
- Detects visually similar images using perceptual hashing (pHash)
- Sort-by-name scanning with configurable neighbor window `k`
- Shows groups one by one and pauses for user decision:
  - **Remove selected** → moves selected files to `removed/`
  - **Skip** → continues scanning
- Double-click a thumbnail to open it via `xdg-open`
- Supports HEIC/HEIF via `pillow-heif` + `libheif`

## Install (Ubuntu)
### System deps (HEIC)
```bash
sudo apt-get update
sudo apt-get install -y libheif1 libheif-dev