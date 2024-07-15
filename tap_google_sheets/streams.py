"""Stream type classes for tap-google-sheets."""

import re
from itertools import zip_longest
from pathlib import Path
from typing import Iterable, List

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_google_sheets.client import GoogleSheetsBaseStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class GoogleSheetsStream(GoogleSheetsBaseStream):
    """Google sheets stream."""

    child_sheet_name = None
    primary_key = None
    url_base = "https://sheets.googleapis.com/v4/spreadsheets"
    stream_config = None

    @property
    def path(self):
        """Set the path for the stream."""
        path = f"/{self.stream_config['sheet_id']}/values/{self.child_sheet_name}"
        sheet_range = self.stream_config.get("range")
        if sheet_range:
            path += f"!{sheet_range}"
        return path

    def get_selected_columns(self) -> List[str]:
        """Extract selected columns from the metadata catalog.

        Returns:
            A list of selected columns.
        """
        selected_columns = []

        for key, metadata in self.metadata.items():
            if "properties" not in key:
                continue

            _, column_name = key
            if metadata.selected:
                selected_columns.append(column_name)

        # Normalize the selected columns
        return list(set(re.sub(r"\s+", "_", col.strip()) for col in selected_columns))

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse response, build response back up into json, update stream schema."""
        headings, *data = response.json()["values"]
        data_rows = []

        # Normalize column headings to match possible user input
        normalized_headings = [re.sub(r"\s+", "_", h.strip()) for h in headings]

        selected_columns = self.get_selected_columns()
        selected_columns_set = set(selected_columns) if selected_columns else set(normalized_headings)

        # List of true and false based if heading has value and is in selected_columns
        mask = [bool(x) and re.sub(r"\s+", "_", x.strip()) in selected_columns_set for x in headings]

        # Build up a json like response using the mask to ignore unnamed columns
        for values in data:
            data_rows.append(
                dict(
                    [(re.sub(r"\s+", "_", h.strip()), v or "") for m, h, v in zip_longest(mask, headings, values) if m]
                )
            )

        # We have to re apply the streams schema for target-postgres
        for stream_map in self.stream_maps:
            if stream_map.stream_alias == self.name:
                stream_map.transformed_schema = self.schema

        # You have to send another schema message as well for target-postgres
        self._write_schema_message()

        yield from extract_jsonpath(self.records_jsonpath, input=data_rows)
