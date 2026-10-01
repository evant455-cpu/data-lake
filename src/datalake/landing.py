"""Lesson 1: landing raw data.

Rule of the raw layer: store exactly what the source gave us, never edit it.
If our cleaning code has a bug later, we fix the code and re-run from raw.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


def land_raw(
    data: bytes,
    *,
    source: str,
    dataset: str,
    filename: str,
    lake_root: Path = Path("lake"),
    now: datetime | None = None,
    extra_meta: dict | None = None,
) -> Path:
    """Save raw bytes under lake/raw/<source>/<dataset>/<YYYY-MM-DD>/<filename>.

    Also writes a small sidecar file, <filename>.meta.json, recording where the
    data came from, when we fetched it, and a checksum (a fingerprint of the
    bytes, so we can later prove the file was not changed).
    `extra_meta` adds more provenance to the sidecar (for example whether a fetch was complete);
    it can never replace the core fields, so the checksum always stays the real one.
    Returns the path of the saved file. Refuses to overwrite an existing file.
    """
    now = now or datetime.now(timezone.utc)
    folder = lake_root / "raw" / source / dataset / now.strftime("%Y-%m-%d")
    folder.mkdir(parents=True, exist_ok=True)

    target = folder / filename
    if target.exists():
        raise FileExistsError(f"Raw file already landed, not overwriting: {target}")

    target.write_bytes(data)
    meta = {
        **(extra_meta or {}),
        "source": source,
        "dataset": dataset,
        "filename": filename,
        "fetched_at_utc": now.isoformat(),
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }
    (folder / f"{filename}.meta.json").write_text(json.dumps(meta, indent=2))
    return target
