import re


def get_parsed_sheet_id(input_string: str) -> str:
    pattern = r"/d/([a-zA-Z0-9-_]+)"

    # Check if the input is a valid spreadsheet ID (assuming a valid ID is between 40-50 characters long)
    if re.fullmatch(r"[a-zA-Z0-9-_]{40,50}", input_string):
        return input_string

    match = re.search(pattern, input_string)
    if match:
        return match.group(1)
    else:
        raise RuntimeError(f"Spreadsheet ID not found in the input: {input_string}.")
