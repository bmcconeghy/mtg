import datetime
import json
import logging
import os
import requests
from enum import StrEnum

import pandas as pd
import typer

logging.basicConfig(encoding="utf-8", level=logging.INFO)
console_handler = logging.StreamHandler()
formatter = logging.Formatter(
    "{asctime} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
console_handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(console_handler)
logger.propagate = False


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
            logger.info(f"Download complete... destination={destination}")
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
    check_cache: bool = True,
) -> str:
    if not local_path:
        cwd = os.getcwd()
        datetime_stamp = datetime.datetime.now().strftime("%Y-%m-%d")
        local_path = f"{cwd}/{datetime_stamp}_scryfall.json"
    if check_cache:
        if os.path.exists(local_path):
            return local_path
    all_dbs = get_database_uris()
    for db in all_dbs["data"]:
        if db["type"] == bulk_data_type:
            uri = db["download_uri"]
            logger.info(f"Downloading database... database={bulk_data_type}, uri={uri}")
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
    assert not os.path.exists(
        output_path
    ), f"{output_path=} already exists! Either delete it, or specify a different file path."
    if not json_db_path:
        json_db_path = download_default_cards()
    df = generate_dataframe_from_db(json_db_path)
    df.to_csv(output_path, index=False)


def get_price_for_finish(s: pd.Series):
    """Map finish type price so we can merge into a single column."""
    if s["finish"] not in Finish:
        logger.error(f"{s['finish']} is not a known finish")
        return None
    if s["finish"] == Finish.nonfoil:
        return s["prices_usd"]
    if s["finish"] == Finish.foil:
        return s["prices_usd_foil"]
    elif s["finish"] == Finish.etched:
        return s["prices_usd_etched"]


@app.command()
def update_collection_with_prices(
    collection_path: str = typer.Option(
        help="Path to Collection CSV. Required columns: card_name, set_name, collector_number, and finish."
    ),
    output_path: str = typer.Option(
        help="Path to  where the updated Collection CSV will be written."
    ),
    json_db_path: str = typer.Option(
        default=None, help="Path to the Scryfall DB on your local machine"
    ),
):
    if not json_db_path:
        db = generate_dataframe_from_db(download_default_cards())
    else:
        db = generate_dataframe_from_db(json_db_path)

    # If there isn't a price, we don't want to see the card
    db.dropna(subset=["prices_usd"], inplace=True)

    collection = pd.read_csv(collection_path)

    # Cast to lower case to ensure a more accurate match
    # Misplaced commas or typos currently not supported
    collection["card_name_lower_case"] = collection["card_name"].str.lower()
    db["card_name_lower_case"] = db["name"].str.lower()

    # TODO: Currently drops all other data found in collection,
    # could change this to keep it and elegantly update?
    merged = collection[
        ["card_name_lower_case", "set_name", "collector_number", "finish"]
    ].merge(db, how="left", on=["card_name_lower_case", "set_name", "collector_number"])

    # Normalize our prices, based on finish
    merged["price"] = merged.apply(get_price_for_finish, axis=1)

    # Subset to columns of interest
    collection_with_prices = merged[
        ["name", "set_name", "collector_number", "finish", "type_line", "cmc", "price"]
    ]

    collection_with_prices.rename(columns={"name": "card_name"}).to_csv(
        output_path, index=False
    )


@app.command()
def generate_set_name_list(
    output_path: str = typer.Option(help="Path to where set name CSV will be written."),
    json_db_path: str = typer.Option(
        default=None, help="Path to the Scryfall DB on your local machine"
    ),
):
    if not json_db_path:
        json_db_path = download_default_cards()
    with open(json_db_path) as f:
        db = json.load(f)

    df = pd.DataFrame({card["set_name"] for card in db}, columns=["set_name"])
    df.to_csv(output_path, index=False)


if __name__ == "__main__":
    app()
