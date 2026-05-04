"""Helpers for temporarily suppressing Eufy devices from Home Assistant."""

from __future__ import annotations

DISABLED_CAMERA_SERIALS_PATH = "/config/codex-eufy-camera-snapshots-disabled-camera-serials.txt"


def disabled_camera_serials() -> set[str]:
    """Return camera serials that should stay hidden during a snapshot run."""
    try:
        with open(DISABLED_CAMERA_SERIALS_PATH, encoding="utf-8") as file:
            return {line.strip() for line in file if line.strip() and not line.startswith("#")}
    except OSError:
        return set()
