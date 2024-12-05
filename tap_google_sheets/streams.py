"""Stream type classes for tap-google-sheets."""

import re
from itertools import zip_longest
from typing import Any, Iterable, List

import requests
from singer_sdk import Tap
from singer_sdk._singerlib import Schema
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_google_sheets.client import GoogleSheetsBaseStream
from tap_google_sheets.utils import get_parsed_sheet_id


class GoogleSheetsStream(GoogleSheetsBaseStream):
    """Google sheets stream."""

    def __init__(
        self,
        tap: Tap,
        tab_name: str,
        name: str | None = None,
        schema: dict[str, Any] | Schema | None = None,
        path: str | None = None,
    ) -> None:
        self.tab_name = tab_name
        super().__init__(tap, name, schema, path)

    @property
    def path(self):
        """Set the path for the stream."""
        path = f"{get_parsed_sheet_id(self.config['sheet_id'])}/values/'{self.tab_name}'"
        sheet_range = self.config.get("range")
        if sheet_range:
            path += f"!{sheet_range}"
        return path

    def get_selected_columns(self) -> List[str]:
        """Extract selected columns from the metadata catalog.

        Returns:
            A list of selected columns.
        """
        return []
        selected_columns = []
        catalog_metadata = self._tap_input_catalog[self.name].metadata

        for key, metadata in catalog_metadata.items():
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

        yield from extract_jsonpath(self.records_jsonpath, input=data_rows)
