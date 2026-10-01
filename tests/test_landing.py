import hashlib
import json
from datetime import datetime, timezone

import pytest

from datalake.landing import land_raw

WHEN = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


def test_lands_file_in_expected_folder(tmp_path):
    p = land_raw(b"abc", source="gfw", dataset="gaps", filename="x.json", lake_root=tmp_path, now=WHEN)
    assert p == tmp_path / "raw" / "gfw" / "gaps" / "2026-09-30" / "x.json"
    assert p.read_bytes() == b"abc"


def test_sidecar_records_checksum_and_source(tmp_path):
    p = land_raw(b"abc", source="gaia", dataset="dr3", filename="t.csv", lake_root=tmp_path, now=WHEN)
    meta = json.loads((p.parent / "t.csv.meta.json").read_text())
    assert meta["sha256"] == hashlib.sha256(b"abc").hexdigest()
    assert meta["source"] == "gaia" and meta["size_bytes"] == 3


def test_never_overwrites_raw(tmp_path):
    land_raw(b"1", source="s", dataset="d", filename="f", lake_root=tmp_path, now=WHEN)
    with pytest.raises(FileExistsError):
        land_raw(b"2", source="s", dataset="d", filename="f", lake_root=tmp_path, now=WHEN)


def test_extra_meta_is_recorded_but_cannot_overwrite_core_fields(tmp_path):
    p = land_raw(b"abc", source="s", dataset="d", filename="f", lake_root=tmp_path, now=WHEN,
                 extra_meta={"complete": False, "rows": 7, "sha256": "forged"})
    meta = json.loads((p.parent / "f.meta.json").read_text())
    assert meta["complete"] is False and meta["rows"] == 7
    assert meta["sha256"] == hashlib.sha256(b"abc").hexdigest()  # the real checksum wins
