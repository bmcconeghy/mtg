import datetime
import json
import logging
import os
import requests
from enum import StrEnum

import pandas as pd
import typer

logger = logging.getLogger()

# Change this to your local path of choice
BASE_DIR = ""

SCRYFALL_URL = "https://api.scryfall.com"
SCRYFALL_USER_AGENT = "GM-APP"
SCRYFALL_ACCEPT = "*/*"


app = typer.Typer()


class BulkDataType(StrEnum):
    oracle_cards = "oracle_cards"
    unique_artwork = "unique_artwork"
    default_cards = "default_cards"
    all_cards = "all_cards"
    rulings = "rulings"


class Finish(StrEnum):
    foil = "foil"
    nonfoil = "nonfoil"
    etched = "etched"


def download_file(url: str, destination: str):
    with requests.get(url, stream=True) as response:
        # Check if the request was successful
        if response.status_code == 200:
            with open(destination, "wb") as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
            logger.info("Download completed", destination=destination)
        else:
            logger.error(
                f"Failed to retrieve the file. HTTP Status: {response.status_code}"
            )


def flatten_dict(d: dict, parent_key: str = "", sep: str = "_") -> dict:
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def find_cards(
    card_name: str,
    set_name: str | None = None,
    finish: Finish | None = None,
    db: list[dict] | None = None,
) -> list:
    # TODO: Add in logic to allow db being passed in
    # TODO: Add logic to parse out the set_name and finish
    card_name = card_name.lower()
    all_cards_with_name = [card for card in db if card_name in card["name"].lower()]

    return all_cards_with_name


def get_database_uris():
    db_uri_response = requests.get(
        f"{SCRYFALL_URL}/bulk-data",
        headers={"User-Agent": SCRYFALL_USER_AGENT, "Accept": SCRYFALL_ACCEPT},
    )
    if db_uri_response.status_code == 200:
        return json.loads(db_uri_response.text)


@app.command()
def download_default_cards(
    # Incorrectly typed here because Typer doesn't support `| None`
    local_path: str = None,
    bulk_data_type: BulkDataType = BulkDataType.default_cards,
) -> str:
    if not local_path:
        cwd = os.getcwd()
        datetime_stamp = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M")
        local_path = f"{cwd}/{datetime_stamp}_scryfall.json"
    all_dbs = get_database_uris()
    for db in all_dbs["data"]:
        if db["type"] == bulk_data_type:
            uri = db["download_uri"]
            logger.info("Downloading database", database=bulk_data_type, uri=uri)
            download_file(uri, local_path)
            return local_path


def generate_dataframe_from_db(db_path: str) -> pd.DataFrame:
    with open(db_path, "rb") as f:
        bulk_data = json.load(f)

    card_records = [flatten_dict(card) for card in bulk_data]

    return pd.DataFrame(card_records)


@app.command()
def convert_json_db_to_csv(
    output_path: str = typer.Option(
        help="Path to where the CSV will be written on your local machine"
    ),
    json_db_path: str = typer.Option(
        default=None, help="Path to the Scryfall DB on your local machine"
    ),
):
    if not json_db_path:
        db_path = download_default_cards()
    df = generate_dataframe_from_db(db_path)
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    app()
