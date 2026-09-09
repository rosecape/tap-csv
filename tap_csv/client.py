"""Custom client handling, including CSVStream base class.

Rosecape fork: adds an S3 / S3-compatible source mode alongside the
upstream local-filesystem behavior. When the stream's `file_config`
contains `s3_bucket`, files are listed and read via boto3 (honoring an
optional `s3_endpoint_url` so DigitalOcean Spaces / MinIO work unchanged).
Local-filesystem behavior is preserved exactly when `s3_bucket` is absent.
"""

from __future__ import annotations

import csv
import io
import os
import re
import typing as t
from datetime import datetime, timezone
from functools import cached_property

from singer_sdk import typing as th
from singer_sdk.streams import Stream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context

SDC_SOURCE_FILE_COLUMN = "_sdc_source_file"
SDC_SOURCE_LINENO_COLUMN = "_sdc_source_lineno"
SDC_SOURCE_FILE_MTIME_COLUMN = "_sdc_source_file_mtime"

# Rosecape fork: NUL bytes are stripped from every source file before parsing.
#
# `csv.reader` raises `_csv.Error: line contains NUL` the moment it meets a
# 0x00, which aborts the whole sync. Worse, because the run dies, the Singer
# bookmark is never advanced — so the stream stays stuck on that file on every
# subsequent run rather than losing a single row.
#
# Exports out of SQL Server hit this routinely: a fixed-width `nchar` column
# that was never written can carry embedded NULs into the CSV. Observed on
# ESA's `mtech.HimSetterInventory` export, where 5.5% of 2,563 objects were
# unreadable (scattered across 2015-2024), while two sibling exports in the
# same bucket were clean.
#
# NUL is not legal payload in a text CSV, so dropping it loses nothing: the row
# and field structure around it is preserved, and every other byte survives.
NUL_TEXT = "\x00"
NUL_BYTES = b"\x00"

# Rosecape fork: top-level S3 settings that act as defaults for every file
# entry. Deployment platforms (Meltano/AIP) inject one env var per setting
# (TAP_CSV_S3_BUCKET, TAP_CSV_S3_ACCESS_KEY_ID, ...), each mapped to a vault
# key — there is no env-var path into a nested `files[i].s3_*` field, so the
# tap merges these top-level values into each file_config. Per-file values
# still win when both are present.
S3_DEFAULT_KEYS = (
    "s3_bucket",
    "s3_prefix",
    "s3_search_pattern",
    "s3_endpoint_url",
    "s3_access_key_id",
    "s3_secret_access_key",
    "s3_region",
    # Rosecape fork: "csv" (default) or "json". JSON mode emits each file as a
    # single `_data` cell (raw JSON string); flattening/typing happens in dbt.
    "s3_format",
)


class CSVStream(Stream):
    """Stream class for CSV streams."""

    def __init__(self, *args: t.Any, **kwargs: t.Any) -> None:
        """Init CSVStram."""
        # cache file_config so we dont need to go iterating the config list again later
        self.file_config = kwargs.pop("file_config")
        # Rosecape fork: capture the tap reference BEFORE super().__init__().
        # The SDK evaluates the (cached) schema during super().__init__(),
        # before self.config is wired up and before the top-level s3_* fold —
        # so is_json/is_s3 need a config source that is available this early.
        # The tap (already constructed) carries the resolved top-level config.
        self._tap_ref = kwargs.get("tap")
        self._file_paths: list[str] = []
        self._header: list[str] | None = None
        # Rosecape fork: S3 object key -> LastModified, populated during
        # listing so get_records can stamp _sdc_source_file_mtime without a
        # second HEAD request per object.
        self._s3_mtimes: dict[str, datetime] = {}
        super().__init__(*args, **kwargs)

        # Rosecape fork: fold top-level S3 settings into this file's config as
        # defaults (per-file values take precedence). Lets AIP inject each S3
        # credential as its own env var instead of a nested `files[]` JSON.
        for key in S3_DEFAULT_KEYS:
            if self.file_config.get(key) in (None, "") and self.config.get(key) not in (
                None,
                "",
            ):
                self.file_config[key] = self.config[key]

        self._primary_keys: list[str] = self.file_config.get("keys", [])

    # ------------------------------------------------------------------
    # Rosecape fork: S3 source helpers
    # ------------------------------------------------------------------

    @property
    def is_s3(self) -> bool:
        """True when this stream reads from S3 instead of local files."""
        return bool(self.file_config.get("s3_bucket"))

    @property
    def is_json(self) -> bool:
        """True when the stream reads JSON files instead of CSV rows.

        Rosecape fork: gated on `s3_format == "json"` (default "csv"). When
        enabled, each file/object is emitted as a single `_data` string cell
        holding the raw JSON; all flattening and type coercion is done
        downstream in dbt (jsonb), mirroring the CSV mode's "emit strings,
        coerce in dbt" philosophy. The CSV code path is untouched.

        Reads `s3_format` from the per-file config first, then falls back to
        the top-level tap config. The fallback matters at construction /
        DISCOVER time: top-level `s3_*` settings (the shape AIP injects via
        TAP_CSV_S3_* env vars) are only folded into file_config AFTER
        super().__init__(), but the SDK evaluates the schema DURING it — so a
        file_config-only read would see "csv" and mis-discover the schema.
        """
        fmt = self.file_config.get("s3_format")
        if fmt in (None, ""):
            # Top-level tap config, via the tap reference captured in __init__
            # (available during super().__init__(), unlike self.config).
            tap_config = getattr(getattr(self, "_tap_ref", None), "config", None)
            if tap_config:
                fmt = tap_config.get("s3_format")
        if fmt in (None, ""):
            try:
                fmt = self.config.get("s3_format")
            except Exception:  # pragma: no cover - defensive; config not ready
                fmt = None
        return str(fmt or "csv").lower() == "json"

    @property
    def _default_search_pattern(self) -> str:
        """Default S3 key basename pattern for the active format."""
        return r".*\.json$" if self.is_json else r".*\.csv$"

    @cached_property
    def _s3_client(self):
        """Build a boto3 S3 client honoring optional endpoint_url + creds.

        boto3 transparently targets `s3_endpoint_url` when set, so the same
        code path serves AWS S3, DigitalOcean Spaces, and MinIO.
        """
        import boto3

        kwargs: dict = {}
        if self.file_config.get("s3_endpoint_url"):
            kwargs["endpoint_url"] = self.file_config["s3_endpoint_url"]
        if self.file_config.get("s3_access_key_id"):
            kwargs["aws_access_key_id"] = self.file_config["s3_access_key_id"]
        if self.file_config.get("s3_secret_access_key"):
            kwargs["aws_secret_access_key"] = self.file_config["s3_secret_access_key"]
        if self.file_config.get("s3_region"):
            kwargs["region_name"] = self.file_config["s3_region"]
        return boto3.client("s3", **kwargs)

    def _list_s3_keys(self) -> list[str]:
        r"""List S3 object keys under the prefix matching the search pattern.

        The pattern is matched against each key's BASENAME (so a
        date-partitioned layout like `prod/YYYY/MM/DD/bluecard.csv` works
        with a pattern of `^bluecard\\.csv$`). Pagination-safe.
        Populates `self._s3_mtimes` as a side effect.
        """
        bucket = self.file_config["s3_bucket"]
        prefix = self.file_config.get("s3_prefix", "") or ""
        pattern = self.file_config.get("s3_search_pattern") or self._default_search_pattern
        compiled = re.compile(pattern)

        keys: list[str] = []
        paginator = self._s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []) or []:
                key = obj["Key"]
                basename = key.rsplit("/", 1)[-1]
                if compiled.search(basename):
                    keys.append(key)
                    lm = obj.get("LastModified")
                    if lm is not None:
                        # boto3 returns tz-aware datetimes already.
                        self._s3_mtimes[key] = lm
        # Deterministic order — sorted by key (date-partitioned paths sort
        # chronologically). Keeps Singer state bookmarks monotonic.
        keys.sort()
        return keys

    def _get_s3_rows(self, key: str) -> t.Iterable[list[t.Any]]:
        """Stream rows from an S3 CSV object via GetObject."""
        bucket = self.file_config["s3_bucket"]
        encoding = self.file_config.get("encoding", "utf-8")
        resp = self._s3_client.get_object(Bucket=bucket, Key=key)
        # Strip NULs before decoding — see NUL_BYTES at the top of this module.
        text = resp["Body"].read().replace(NUL_BYTES, b"").decode(encoding)
        csv.register_dialect(
            "tap_dialect",
            delimiter=self.file_config.get("delimiter", ","),
            doublequote=self.file_config.get("doublequote", True),
            escapechar=self.file_config.get("escapechar", None),
            quotechar=self.file_config.get("quotechar", '"'),
            skipinitialspace=self.file_config.get("skipinitialspace", False),
            strict=self.file_config.get("strict", False),
        )
        yield from csv.reader(io.StringIO(text), dialect="tap_dialect")

    def _read_file_text(self, file_path: str) -> str:
        """Read a whole file/object as text (used by JSON source mode).

        Rosecape fork: serves both S3 (boto3 GetObject) and local paths.
        """
        encoding = self.file_config.get("encoding") or "utf-8"
        if self.is_s3:
            bucket = self.file_config["s3_bucket"]
            resp = self._s3_client.get_object(Bucket=bucket, Key=file_path)
            return resp["Body"].read().replace(NUL_BYTES, b"").decode(encoding)
        with open(file_path, encoding=encoding) as f:
            return f.read().replace(NUL_TEXT, "")

    # ------------------------------------------------------------------

    def get_records(self, context: Context | None) -> t.Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        header = self.header

        # Rosecape fork: bookmark-aware file skipping. Upstream tap-csv
        # re-reads every file on every run (it only declares CATALOG +
        # DISCOVER, no STATE). When the stream is configured INCREMENTAL on
        # `_sdc_source_file_mtime`, we skip files whose mtime is strictly
        # older than the bookmark. Files AT the bookmark mtime are re-emitted
        # (Singer `>=` semantics) so a boundary file is never lost; target
        # MERGE on the primary key keeps this idempotent.
        starting_mtime: datetime | None = None
        if self.replication_key == SDC_SOURCE_FILE_MTIME_COLUMN:
            try:
                starting_mtime = self.get_starting_timestamp(context)
            except Exception:  # pragma: no cover - defensive; e.g. no state yet
                starting_mtime = None

        for file_path in self.get_file_paths():
            if self.is_s3:
                # mtime captured during listing (S3 LastModified).
                file_last_modified = self._s3_mtimes.get(
                    file_path, datetime.now(timezone.utc)
                )
            else:
                file_last_modified = datetime.fromtimestamp(
                    os.path.getmtime(file_path), timezone.utc
                )

            # Skip files strictly older than the bookmark (already ingested).
            if starting_mtime is not None and file_last_modified < starting_mtime:
                self.logger.info(
                    "Skipping %s (mtime %s < bookmark %s)",
                    file_path, file_last_modified.isoformat(),
                    starting_mtime.isoformat(),
                )
                continue

            file_lineno = -1

            for row in self.get_rows(file_path):
                file_lineno += 1

                if not file_lineno:
                    continue

                if self.config.get("add_metadata_columns", False):
                    row = [file_path, file_last_modified, file_lineno, *row]

                yield dict(zip(header, row))

    def _get_recursive_file_paths(self, file_path: str) -> list:
        file_paths = []

        for dirpath, _, filenames in os.walk(file_path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                if self.is_valid_filename(file_path):
                    file_paths.append(file_path)

        return file_paths

    @property
    def file_paths(self) -> list[str]:
        """Return the file paths of the stream."""
        return self._file_paths

    @file_paths.setter
    def file_paths(self, paths: list[str]) -> None:
        """Set the file paths of the stream."""
        self._file_paths = paths

    def get_file_paths(self) -> list[str]:
        """Return a list of file paths (local) or object keys (S3) to read.

        This tap accepts file names and directories so it will detect
        directories and iterate files inside. In S3 mode it lists object
        keys under the configured prefix matching the search pattern.
        """
        # Cache file paths so we dont have to iterate multiple times
        if self.file_paths:
            return self.file_paths

        # Rosecape fork: S3 source mode.
        if self.is_s3:
            keys = self._list_s3_keys()
            if not keys:
                bucket = self.file_config["s3_bucket"]
                prefix = self.file_config.get("s3_prefix", "")
                pattern = self.file_config.get(
                    "s3_search_pattern", self._default_search_pattern
                )
                raise RuntimeError(
                    f"Stream '{self.name}' matched no S3 objects in "
                    f"s3://{bucket}/{prefix} with pattern {pattern}."
                )
            self.file_paths = keys
            return keys

        file_path = self.file_config["path"]
        if not os.path.exists(file_path):
            raise Exception(f"File path does not exist {file_path}")

        file_paths = []
        if os.path.isdir(file_path):
            clean_file_path = os.path.normpath(file_path) + os.sep
            file_paths = self._get_recursive_file_paths(clean_file_path)
        elif self.is_valid_filename(file_path):
            file_paths.append(file_path)

        if not file_paths:
            raise RuntimeError(
                f"Stream '{self.name}' has no acceptable files. \
                    See warning for more detail."
            )
        self.file_paths = file_paths
        return file_paths

    def is_valid_filename(self, file_path: str) -> bool:
        """Return whether the file has the expected extension for the format."""
        # Rosecape fork: JSON mode accepts .json; CSV mode unchanged (.csv).
        ext = ".json" if self.is_json else ".csv"
        is_valid = True
        if not file_path.endswith(ext):
            is_valid = False
            self.logger.warning(f"Skipping non-{ext[1:]} file '{file_path}'")
            self.logger.warning(
                f"Please provide a {ext[1:].upper()} file that ends with '{ext}'."
            )
        return is_valid

    def get_rows(self, file_path: str) -> t.Iterable[list[t.Any]]:
        """Return a generator of the rows in a particular CSV/JSON file/object."""
        # Rosecape fork: JSON source mode. Emit the whole file as a single
        # `_data` cell — a header row (`_data`) followed by one data row (the
        # raw JSON text). This reuses get_records' header-skip + dict(zip(...))
        # machinery unchanged; the CSV code path below is untouched. dbt does
        # the jsonb flattening / typing downstream.
        if self.is_json:
            yield ["_data"]
            yield [self._read_file_text(file_path)]
            return

        # Rosecape fork: S3 source mode reads via boto3 GetObject.
        if self.is_s3:
            yield from self._get_s3_rows(file_path)
            return

        encoding = self.file_config.get("encoding", None)
        csv.register_dialect(
            "tap_dialect",
            delimiter=self.file_config.get("delimiter", ","),
            doublequote=self.file_config.get("doublequote", True),
            escapechar=self.file_config.get("escapechar", None),
            quotechar=self.file_config.get("quotechar", '"'),
            skipinitialspace=self.file_config.get("skipinitialspace", False),
            strict=self.file_config.get("strict", False),
        )
        with open(file_path, encoding=encoding) as f:
            # Strip NULs line by line — see NUL_TEXT at the top of this module.
            # csv.reader takes any iterable of strings, so filtering this way
            # keeps the read streaming instead of slurping the whole file.
            yield from csv.reader(
                (line.replace(NUL_TEXT, "") for line in f),
                dialect="tap_dialect",
            )

    @property
    def header(self) -> list[str]:
        """Parse the header of the CSV file (or the fixed JSON column set)."""
        if self._header is not None:
            return self._header

        if self.is_json:
            # Rosecape fork: JSON mode has a fixed single-column shape
            # (`_data`), so the header is known without reading a file. This
            # also lets DISCOVER compute the schema WITHOUT listing S3 at
            # construction time — where top-level s3_bucket isn't folded into
            # file_config yet (see is_json). CSV mode is unchanged below.
            names = ["_data"]
        else:
            first_file = self.get_file_paths()[0]
            for row in self.get_rows(first_file):
                names = [str(col) for col in row]
                break

        if self.config.get("add_metadata_columns", False):
            names = [
                SDC_SOURCE_FILE_COLUMN,
                SDC_SOURCE_FILE_MTIME_COLUMN,
                SDC_SOURCE_LINENO_COLUMN,
                *names,
            ]
        self._header = names
        return names

    @cached_property
    def schema(self) -> dict:
        """Return dictionary of record schema.

        Dynamically detect the json schema for the stream.

        This property is accessed multiple times for each record
        so it's important to cache the result.
        """
        properties: list[th.Property] = []
        properties.extend(
            th.Property(column, th.StringType()) for column in self.header
        )
        # If enabled, add file's metadata to output
        if self.config.get("add_metadata_columns", False):
            properties.extend(
                (
                    th.Property(SDC_SOURCE_FILE_COLUMN, th.StringType),
                    th.Property(SDC_SOURCE_FILE_MTIME_COLUMN, th.DateTimeType),
                    th.Property(SDC_SOURCE_LINENO_COLUMN, th.IntegerType),
                )
            )

        return th.PropertiesList(*properties).to_dict()
