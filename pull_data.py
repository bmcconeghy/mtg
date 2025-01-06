import json
import requests
from enum import StrEnum

import pandas as pd
import structlog
from rich.pretty import pprint

logger = structlog.get_logger()

# Change this to your local path of choice
BASE_DIR = ""

SCRYFALL_URL = "https://api.scryfall.com"
SCRYFALL_USER_AGENT = "GM-APP"
SCRYFALL_ACCEPT = "*/*"


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
            with open(destination, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        file.write(chunk)
            logger.info("Download completed", destination=destination)
        else:
            logger.error(f"Failed to retrieve the file. HTTP Status: {response.status_code}")


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
    card_name: str, set_name: str | None = None, finish: Finish | None = None, database: list[dict] | None = None
) -> list:
    if not database:

    card_name = card_name.lower()
    set_name = set_name.lower()

    all_cards_with_name = [card for card in BULK if card_name in card["name"].lower()]


def get_database_uris():
    db_uri_response = requests.get(f"{SCRYFALL_URL}/bulk-data", headers={"User-Agent": SCRYFALL_USER_AGENT, "Accept": SCRYFALL_ACCEPT})
    if db_uri_response.status_code == 200:
        return json.loads(db_uri_response.text)


def download_default_cards(local_path: str, bulk_data_type: BulkDataType = BulkDataType.default_cards):
    all_dbs = get_database_uris()
    for db in all_dbs["data"]:
        if db["type"] == bulk_data_type:
            uri = db["download_uri"]
            logger.info("Downloading database", database=bulk_data_type, uri=uri)
            download_file(uri, local_path)

with open(f"{BASE_DIR}/default-cards-20241231100753.json") as f:
    BULK = json.load(f)

card_records = [flatten_dict(card) for card in BULK]

unique_set_names = pd.DataFrame(
    sorted(set([cr["set_name"] for cr in BULK])), columns=["Set Name"]
)
unique_set_names.to_csv(f"{BASE_DIR}/set_names.csv", index=False)

df = pd.DataFrame(card_records)

df.to_csv(f"{BASE_DIR}/default-cards-20241231100753.csv", index=False)

df_usd_only = df[
    df[["prices_usd", "prices_usd_foil", "prices_usd_etched"]].notna().any(axis=1)
]

df_usd_only.to_csv(
    f"{BASE_DIR}/default-cards-20241231100753_USD_only.csv", index=False
)


# New bulk data for 2025-01-01

with open(f"{BASE_DIR}/default-cards-20250101100710.json") as f:
    NEWER_BULK = json.load(f)

new_df = pd.DataFrame([flatten_dict(card) for card in NEWER_BULK])
new_df_usd_only = df[
    new_df[["prices_usd", "prices_usd_foil", "prices_usd_etched"]].notna().any(axis=1)
]
merged = pd.merge(df_usd_only, new_df_usd_only, on="id", suffixes=["_old", "_new"])
