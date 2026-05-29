"""Tests for the rosecape-fork S3 / S3-compatible source mode.

boto3 is mocked with a hand-rolled fake so the tests stay hermetic (no
network, no AWS/Spaces credentials needed).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from tap_csv.tap import CSVStream, TapCSV

# ---------------------------------------------------------------------------
# Hermetic S3 fake
# ---------------------------------------------------------------------------


@dataclass
class _Obj:
    """A fake S3 object: key, body bytes, and a LastModified timestamp."""

    key: str
    body: bytes
    last_modified: datetime


class _Body:
    """Stand-in for the StreamingBody returned by boto3 get_object."""

    def __init__(self, data: bytes):
        """Store the raw object bytes."""
        self._data = data

    def read(self) -> bytes:
        """Return the full object body."""
        return self._data


class _Paginator:
    """Stand-in for a boto3 list_objects_v2 paginator."""

    def __init__(self, pages):
        """Store the canned list of result pages."""
        self._pages = pages

    def paginate(self, **kwargs):
        """Yield the canned pages, ignoring Bucket/Prefix kwargs."""
        return iter(self._pages)


class FakeS3:
    """Minimal in-memory boto3 S3 client covering the calls the tap makes."""

    def __init__(self, objects: list[_Obj]):
        """Store the in-memory object list."""
        self._objects = objects

    def get_paginator(self, op):
        """Return a paginator yielding one page of all objects."""
        assert op == "list_objects_v2"
        contents = [
            {"Key": o.key, "Size": len(o.body), "LastModified": o.last_modified}
            for o in self._objects
        ]
        return _Paginator([{"Contents": contents}])

    def get_object(self, *, Bucket, Key):
        """Return the object body for the given Key."""
        for o in self._objects:
            if o.key == Key:
                return {"Body": _Body(o.body)}
        raise KeyError(Key)


_CSV = (
    b"TransID,EntityID,Value\n"
    b"1,a,10\n"
    b"2,b,20\n"
)


def _make_stream(objects, *, add_metadata=True, replication_key=None, state=None,
                 monkeypatch=None, cfg=None):
    """Build a CSVStream wired to a FakeS3.

    boto3.client is patched so that ANY S3 client the stream constructs
    (including during schema discovery in the Tap constructor) returns the
    fake — never touching the network.
    """
    import boto3

    fake = FakeS3(objects)
    monkeypatch.setattr(boto3, "client", lambda *a, **k: fake)

    if cfg is None:
        cfg = {
            "add_metadata_columns": add_metadata,
            "files": [{
                "entity": "thing",
                "keys": ["TransID"],
                "s3_bucket": "test-bucket",
                "s3_prefix": "prod/",
                "s3_search_pattern": r"^data\.csv$",
                "s3_endpoint_url": "https://example.spaces.test",
                "s3_region": "tor1",
                "s3_access_key_id": "fake",
                "s3_secret_access_key": "fake",
            }],
        }
    tap = TapCSV(config=cfg, catalog={}, state=state or {})
    stream = CSVStream(tap=tap, name="thing", file_config=cfg["files"][0])
    if replication_key:
        stream.replication_key = replication_key
        # Mirror the sync lifecycle: Stream.sync() calls this to seed the
        # `starting_replication_value` marker from the prior run's
        # `replication_key_value`. get_records reads it via
        # get_starting_timestamp; calling get_records directly (as these
        # tests do) skips sync(), so seed it here.
        stream._write_starting_replication_value(None)
    return stream


def test_is_s3_true_when_bucket_set(monkeypatch):
    """is_s3 is True when the file config carries an s3_bucket."""
    # One matching object so the Tap constructor's schema discovery
    # (which lists S3 + reads the header) succeeds.
    objs = [_Obj("prod/2026/04/10/data.csv", _CSV,
                 datetime(2026, 4, 10, tzinfo=timezone.utc))]
    s = _make_stream(objs, monkeypatch=monkeypatch)
    assert s.is_s3 is True


def test_list_s3_keys_filters_by_basename_pattern(monkeypatch):
    """Listing keeps only objects whose basename matches the pattern."""
    objs = [
        _Obj("prod/2026/04/10/data.csv", _CSV, datetime(2026, 4, 10, tzinfo=timezone.utc)),
        _Obj("prod/2026/04/11/data.csv", _CSV, datetime(2026, 4, 11, tzinfo=timezone.utc)),
        _Obj("prod/2026/04/11/README.txt", b"x", datetime(2026, 4, 11, tzinfo=timezone.utc)),
    ]
    s = _make_stream(objs, monkeypatch=monkeypatch)
    keys = s.get_file_paths()
    # Only the two data.csv objects, sorted by key (chronological).
    assert keys == [
        "prod/2026/04/10/data.csv",
        "prod/2026/04/11/data.csv",
    ]


def test_s3_records_include_metadata_and_mtime(monkeypatch):
    """Records carry _sdc_source_file and the S3 LastModified mtime."""
    ts = datetime(2026, 4, 10, 15, 0, 0, tzinfo=timezone.utc)
    objs = [_Obj("prod/2026/04/10/data.csv", _CSV, ts)]
    s = _make_stream(objs, add_metadata=True, monkeypatch=monkeypatch)
    records = list(s.get_records(None))
    assert len(records) == 2
    r0 = records[0]
    assert r0["TransID"] == "1"
    assert r0["_sdc_source_file"] == "prod/2026/04/10/data.csv"
    # mtime stamped from the S3 LastModified, not local fs.
    assert r0["_sdc_source_file_mtime"] == ts


def test_s3_incremental_skips_files_older_than_bookmark(monkeypatch):
    """Files strictly older than the bookmark mtime are skipped."""
    older = datetime(2026, 4, 9, tzinfo=timezone.utc)
    boundary = datetime(2026, 4, 10, tzinfo=timezone.utc)
    newer = datetime(2026, 4, 11, tzinfo=timezone.utc)
    objs = [
        _Obj("prod/2026/04/09/data.csv", _CSV, older),
        _Obj("prod/2026/04/10/data.csv", _CSV, boundary),
        _Obj("prod/2026/04/11/data.csv", _CSV, newer),
    ]
    # Bookmark = boundary mtime. Singer >= semantics: skip strictly older,
    # re-emit boundary + newer.
    state = {
        "bookmarks": {
            "thing": {
                "replication_key": "_sdc_source_file_mtime",
                "replication_key_value": boundary.isoformat(),
            }
        }
    }
    s = _make_stream(objs, replication_key="_sdc_source_file_mtime", state=state,
                     monkeypatch=monkeypatch)
    records = list(s.get_records(None))
    files = {r["_sdc_source_file"] for r in records}
    # The 2026-04-09 file is strictly older -> skipped.
    assert "prod/2026/04/09/data.csv" not in files
    # Boundary + newer re-emitted.
    assert files == {
        "prod/2026/04/10/data.csv",
        "prod/2026/04/11/data.csv",
    }


def test_s3_no_bookmark_reads_all_files(monkeypatch):
    """With no prior bookmark, every matching file is read."""
    objs = [
        _Obj("prod/2026/04/09/data.csv", _CSV, datetime(2026, 4, 9, tzinfo=timezone.utc)),
        _Obj("prod/2026/04/10/data.csv", _CSV, datetime(2026, 4, 10, tzinfo=timezone.utc)),
    ]
    s = _make_stream(objs, replication_key="_sdc_source_file_mtime", state={},
                     monkeypatch=monkeypatch)
    records = list(s.get_records(None))
    assert len(records) == 4  # 2 rows in each of 2 files


def test_s3_header_parsed_from_first_object(monkeypatch):
    """The header row is parsed from the first matching S3 object."""
    objs = [_Obj("prod/2026/04/10/data.csv", _CSV, datetime(2026, 4, 10, tzinfo=timezone.utc))]
    s = _make_stream(objs, add_metadata=False, monkeypatch=monkeypatch)
    assert s.header == ["TransID", "EntityID", "Value"]


def test_top_level_s3_settings_merge_into_file_config(monkeypatch):
    """Top-level s3_* settings act as defaults for a file entry that omits them."""
    objs = [_Obj("prod/2026/04/10/data.csv", _CSV,
                 datetime(2026, 4, 10, tzinfo=timezone.utc))]
    # `files` carries only entity + keys; S3 connection settings live at the
    # top level (the shape AIP/Meltano inject via TAP_CSV_S3_* env vars).
    cfg = {
        "add_metadata_columns": True,
        "s3_bucket": "test-bucket",
        "s3_prefix": "prod/",
        "s3_search_pattern": r"^data\.csv$",
        "s3_endpoint_url": "https://example.spaces.test",
        "s3_region": "tor1",
        "s3_access_key_id": "fake",
        "s3_secret_access_key": "fake",
        "files": [{"entity": "thing", "keys": ["TransID"]}],
    }
    s = _make_stream(objs, monkeypatch=monkeypatch, cfg=cfg)
    assert s.is_s3 is True
    assert s.file_config["s3_bucket"] == "test-bucket"
    assert s.get_file_paths() == ["prod/2026/04/10/data.csv"]


def test_per_file_s3_setting_overrides_top_level(monkeypatch):
    """A per-file s3_* value wins over the top-level default."""
    objs = [_Obj("prod/2026/04/10/data.csv", _CSV,
                 datetime(2026, 4, 10, tzinfo=timezone.utc))]
    cfg = {
        "add_metadata_columns": True,
        "s3_bucket": "top-level-bucket",
        "s3_prefix": "prod/",
        "s3_search_pattern": r"^data\.csv$",
        "s3_endpoint_url": "https://example.spaces.test",
        "s3_region": "tor1",
        "s3_access_key_id": "fake",
        "s3_secret_access_key": "fake",
        "files": [{"entity": "thing", "keys": ["TransID"],
                   "s3_bucket": "per-file-bucket"}],
    }
    s = _make_stream(objs, monkeypatch=monkeypatch, cfg=cfg)
    assert s.file_config["s3_bucket"] == "per-file-bucket"
