"""google_sheets tap class."""

import re
from http import HTTPStatus
from typing import List

import requests
from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from slugify import slugify

from tap_google_sheets.client import GoogleSheetsClient
from tap_google_sheets.streams import GoogleSheetsStream
from tap_google_sheets.utils import get_parsed_sheet_id


class TapGoogleSheets(Tap):
    """google_sheets tap class."""

    name = "tap-google-sheets"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "oauth_credentials.client_id",
            th.StringType,
            description="Your google client_id",
        ),
        th.Property(
            "oauth_credentials.client_secret",
            th.StringType,
            description="Your google client_secret",
        ),
        th.Property(
            "oauth_credentials.refresh_token",
            th.StringType,
            description="Your google refresh token",
        ),
        th.Property(
            "sheet_link",
            th.StringType,
            description="Your google sheet link",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        streams: List[Stream] = []
        available_tabs: List[str] = self.get_available_tabs()
        for tab in available_tabs:
            sheet_data = self.get_sheet_data(sheet_name=tab)
            stream_schema = self.get_schema(sheet_data)
            stream = GoogleSheetsStream(
                tap=self, tab_name=tab, name=slugify(tab, separator="_", lowercase=True), schema=stream_schema
            )
            streams.append(stream)

        return streams

    def get_available_tabs(self):
        config_stream = GoogleSheetsClient(
            tap=self,
            name="config",
            schema={"one": "one"},
            path=get_parsed_sheet_id(self.config["sheet_link"]),
        )

        prepared_request = config_stream.prepare_request(None, None)

        response: requests.Response = config_stream._request(prepared_request, None)

        if response.status_code == HTTPStatus.OK:
            data = response.json()
            sheet_names = [sheet["properties"]["title"] for sheet in data["sheets"]]
            return sheet_names
        else:
            raise Exception(f"Failed to retrieve data: {response.status_code} - {response.text}")

    def get_schema(self, google_sheet_data: requests.Response):
        """Build the schema from the data returned by the google sheet."""
        headings, *data = google_sheet_data.json()["values"]

        schema = th.PropertiesList()
        for column in headings:
            if column:
                schema.append(th.Property(re.sub(r"\s+", "_", column.strip()), th.StringType))

        return schema.to_dict()

    def get_sheet_data(self, sheet_name):
        """Get the data from the selected or first visible sheet in the google sheet."""
        config_stream = GoogleSheetsClient(
            tap=self,
            name="config",
            schema={"not": "null"},
            path=get_parsed_sheet_id(self.config["sheet_link"]) + "/values/" + "'" + sheet_name + "'",
        )

        prepared_request = config_stream.prepare_request(None, None)
        response: requests.Response = config_stream._request(prepared_request, None)

        return response


if __name__ == "__main__":
    TapGoogleSheets.cli()
