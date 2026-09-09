"""Tests client methods."""

from __future__ import annotations

import os

from tap_csv.tap import CSVStream, TapCSV


def test_get_file_paths_recursively():
    """Test get file paths recursively."""
    test_data_dir = os.path.dirname(os.path.abspath(__file__))

    SAMPLE_CONFIG = {
        "files": [
            {
                "entity": "test",
                "path": f"{test_data_dir}/data/subfolder1/",
                "keys": [],
            }
        ]
    }

    stream = CSVStream(
        tap=TapCSV(config=SAMPLE_CONFIG, catalog={}, state={}),
        name="test_recursive",
        file_config=SAMPLE_CONFIG.get("files")[0],
    )
    assert stream.get_file_paths() == [
        f"{test_data_dir}/data/subfolder1/alphabet.csv",
        f"{test_data_dir}/data/subfolder1/subfolder2/alphabet.csv",
    ]


def test_local_csv_with_nul_bytes_is_read(tmp_path):
    """A local CSV containing NULs parses instead of raising _csv.Error.

    Mirrors the S3 behavior — the local read path filters NULs line by line so
    the read stays streaming rather than slurping the file into memory.
    """
    csv_path = tmp_path / "nul.csv"
    csv_path.write_bytes(
        b"id,name,qty\n"
        b"1,alpha,10\n"
        b"2,beta\x00\x00,20\n"
    )

    config = {"files": [{"entity": "test", "path": str(csv_path), "keys": ["id"]}]}
    stream = CSVStream(
        tap=TapCSV(config=config, catalog={}, state={}),
        name="test",
        file_config=config["files"][0],
    )

    rows = list(stream.get_rows(str(csv_path)))

    assert rows == [["id", "name", "qty"], ["1", "alpha", "10"], ["2", "beta", "20"]]
