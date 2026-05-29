"""CSV tap class."""

from __future__ import annotations

import json
import os

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.helpers._classproperty import classproperty
from singer_sdk.helpers.capabilities import TapCapabilities

from tap_csv.client import CSVStream


class TapCSV(Tap):
    """CSV tap class."""

    name = "tap-csv"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "files",
            th.ArrayType(
                th.ObjectType(
                    th.Property("entity", th.StringType, required=True),
                    # `path` is required for LOCAL file sources. For S3 sources
                    # (when `s3_bucket` is set) it is ignored — leave it empty
                    # or omit it. Kept non-required so S3-only configs validate.
                    th.Property("path", th.StringType, required=False),
                    th.Property("keys", th.ArrayType(th.StringType), required=True),
                    th.Property(
                        "encoding", th.StringType, required=False, default="utf-8"
                    ),
                    th.Property("delimiter", th.StringType, required=False),
                    th.Property("doublequote", th.BooleanType, required=False),
                    th.Property("escapechar", th.StringType, required=False),
                    th.Property("quotechar", th.StringType, required=False),
                    th.Property("skipinitialspace", th.BooleanType, required=False),
                    th.Property("strict", th.BooleanType, required=False),
                    # --- rosecape fork: S3 / S3-compatible source ---
                    # When `s3_bucket` is present, the stream reads CSV objects
                    # from S3 (or an S3-compatible store like DigitalOcean
                    # Spaces / MinIO via `s3_endpoint_url`) instead of the
                    # local filesystem. All `s3_*` fields below are scoped to
                    # the individual file entry so different streams can target
                    # different buckets.
                    th.Property("s3_bucket", th.StringType, required=False),
                    th.Property(
                        "s3_prefix", th.StringType, required=False, default=""
                    ),
                    th.Property(
                        "s3_search_pattern",
                        th.StringType,
                        required=False,
                        description=(
                            "Regex matched against each object key's basename. "
                            "Defaults to '.*\\.csv$'."
                        ),
                    ),
                    th.Property("s3_endpoint_url", th.StringType, required=False),
                    th.Property("s3_access_key_id", th.StringType, required=False),
                    th.Property(
                        "s3_secret_access_key", th.StringType, required=False
                    ),
                    th.Property("s3_region", th.StringType, required=False),
                )
            ),
            description="An array of csv file stream settings.",
        ),
        th.Property(
            "csv_files_definition",
            th.StringType,
            description="A path to the JSON file holding an array of file settings.",
        ),
        th.Property(
            "add_metadata_columns",
            th.BooleanType,
            required=False,
            default=False,
            description=(
                "When True, add the metadata columns (`_sdc_source_file`, "
                "`_sdc_source_file_mtime`, `_sdc_source_lineno`) to output."
            ),
        ),
        # --- rosecape fork: top-level S3 defaults ---
        # These mirror the per-file `s3_*` fields and act as defaults for
        # every file entry (a per-file value overrides the top-level one).
        # Deployment platforms map each to a single env var
        # (TAP_CSV_S3_BUCKET, TAP_CSV_S3_ACCESS_KEY_ID, ...) backed by one
        # vault key — there is no env-var path into a nested `files[i].s3_*`
        # field. For a single-stream S3 source, set these and give `files`
        # just `entity` + `keys`.
        th.Property("s3_bucket", th.StringType, required=False),
        th.Property("s3_prefix", th.StringType, required=False),
        th.Property("s3_search_pattern", th.StringType, required=False),
        th.Property("s3_endpoint_url", th.StringType, required=False),
        th.Property("s3_access_key_id", th.StringType, required=False),
        th.Property("s3_secret_access_key", th.StringType, required=False),
        th.Property("s3_region", th.StringType, required=False),
    ).to_dict()

    @classproperty
    def capabilities(self) -> list[TapCapabilities]:
        """Get tap capabilities."""
        return [
            TapCapabilities.CATALOG,
            TapCapabilities.DISCOVER,
            # Rosecape fork: file-level INCREMENTAL on _sdc_source_file_mtime
            # (bookmark-aware file skipping in CSVStream.get_records).
            TapCapabilities.STATE,
        ]

    def get_file_configs(self) -> list[dict]:
        """Return a list of file configs.

        Either directly from the config.json or in an external file
        defined by csv_files_definition.
        """
        csv_files = self.config.get("files")
        csv_files_definition = self.config.get("csv_files_definition")
        if csv_files_definition:
            if os.path.isfile(csv_files_definition):
                with open(csv_files_definition) as f:
                    csv_files = json.load(f)
            else:
                self.logger.error(f"tap-csv: '{csv_files_definition}' file not found")
                exit(1)
        if not csv_files:
            self.logger.error("No CSV file definitions found.")
            exit(1)
        return csv_files

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams."""
        return [
            CSVStream(
                tap=self,
                name=file_config.get("entity"),
                file_config=file_config,
            )
            for file_config in self.get_file_configs()
        ]


if __name__ == "__main__":
    TapCSV.cli()
