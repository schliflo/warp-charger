"""NFC new-scan detection, mirroring the firmware's web UI logic."""

from typing import Any

DETECTION_THRESHOLD_MS = 2000


class NfcScanDetector:
    """Detect new tag scans from successive ``nfc/seen_tags`` payloads."""

    def __init__(self) -> None:
        self._prev: dict[tuple[int, str], int] | None = None

    def reset(self) -> None:
        """Forget previous state; the next feed only records."""
        self._prev = None

    def feed(self, seen_tags: list[dict[str, Any]]) -> list[tuple[int, str]]:
        """Return ``(tag_type, tag_id)`` of newly scanned tags, newest first."""
        current: dict[tuple[int, str], int] = {}
        for tag in seen_tags:
            tag_id = tag.get("tag_id")
            if not tag_id:
                continue
            key = (int(tag["tag_type"]), tag_id)
            age = int(tag["last_seen"])
            if key not in current or age < current[key]:
                current[key] = age

        prev, self._prev = self._prev, current
        if prev is None:
            return []

        new = [
            (age, key)
            for key, age in current.items()
            if age < DETECTION_THRESHOLD_MS
            and (key not in prev or prev[key] >= DETECTION_THRESHOLD_MS)
        ]
        new.sort(key=lambda item: item[0])
        return [key for _, key in new]
