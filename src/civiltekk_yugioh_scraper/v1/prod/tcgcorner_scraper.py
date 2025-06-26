from io import StringIO
import json
import requests
import csv
import re

from ..utilities.aws_utilities import save_to_s3
from ..config import DEFAULT_CARD_QUANTITY_INTERVAL, BUCKET_NAME


def get_tcgcorner_url(page_number: int):
    """_summary_

    Parameters
    ----------
    page_number : int
        default value: https://filter-v9.globosoftware.net/filter?shop=6b44fd.myshopify.com&collection=462781972755&event=init&cid=&did=&page_type=collection&limit=250&page=1

    Returns
    -------
    _type_
        _description_
    """
    return 'https://filter-v9.globosoftware.net/filter?shop=6b44fd.myshopify.com&collection=462781972755&event=init&page_type=collection&limit={quantity}&page={page_number}&currency=SGD_SG&country=SG'.format(page_number=page_number, quantity=DEFAULT_CARD_QUANTITY_INTERVAL)


def replace_card_rarity_name(tcgcorner_rarity: str):
    rarity_dict = {
        "QSCR": "Quarter Century Secret Rare",
        "N": "Common",
        "R": "Rare",
        "QCSR": "Quarter Century Secret Rare",
        "SR": "Super Rare",
        "HR": "Holographic Rare",
        "UR": "Ultra Rare",
        "EXSER": "Extra Secret Rare",
        "CR": "Collector's Rare",
        "SER": "Secret Rare",
        "UL": "Ultimate Rare",
        "P-SER": "Secret Parallel Rare"
    }

    if tcgcorner_rarity in rarity_dict:
        return rarity_dict[tcgcorner_rarity]
    else:
        return tcgcorner_rarity


def replace_tcgcorner_set_name(set_name: str | None):
    set_dict = {
        "RARITY COLLECTION - QUARTER CENTURY EDITION - (RC04)": "Rarity Collection Quarter Century Edition",
        "AGOV": "Age of Overlord",
        "CR03": "Creation Pack 03",
        "LEDE-JP": "Legacy of Destruction",
        "24PP": "Premium Pack 2024",
        "Side Unity": "Quarter Century Chronicle side:Unity"
    }
    if set_name in set_dict:
        return set_dict[set_name]
    else:
        return set_name


def check_region(set_card_code_updated: str | None):
    if set_card_code_updated is None:
        return None
    if "AE" in set_card_code_updated:
        return "AE"
    if "JP" in set_card_code_updated:
        return "JP"
    return None


def tcgcorner_scrape_per_page(page_number=1) -> tuple[list[dict], int]:
    # API endpoint
    tcg_array: list[dict] = []
    tcgcorner_url = get_tcgcorner_url(page_number)
    # Make the API call
    response = requests.get(tcgcorner_url, timeout=10)
    data = response.json()
    pagination_data = data['pagination']
    last_page = pagination_data['last_page']

    # Regular expression to split the title
    # title_regex_1 = r"(\w+-\w+)\s+(.*?)\s+\((\w+)\)"
    title_regex_1 = r"(\w+-\w+) (.+) \((.+)\)"

    # Process each product
    for item in data['products']:
        obj = {}
        title: str = item['title']
        price: int | float = item['variants'][0]['price'] if isinstance(item['variants'], list) and len(
            item['variants']) > 0 else 0
        # Concatenate all collection names
        card_sets = [collection['title'] for collection in item['collections'] if collection['title'] not in (
            "Yu-Gi-Oh! Single Card (Asia English)", "All Single Card", "Featured Single Card", "OP05", "FB01", "Yu-Gi-Oh! Single Card (Japanese)")]
        card_set: str | None = card_sets[0] if card_sets else None
        match = re.match(title_regex_1, title)

        if match:
            card_code, card_name, rarity = match.groups()
            # Write the data to the CSV file
            obj['set_card_name_combined'] = card_name
            obj['set_name'] = replace_tcgcorner_set_name(card_set)
            obj['set_card_code_updated'] = card_code
            obj['rarity_name'] = replace_card_rarity_name(rarity)
            obj['price'] = price
            obj['region'] = check_region(set_card_code_updated=card_code)
            tcg_array.append(obj.copy())
        else:
            print(f"Title format mismatch: {title}")

    return tcg_array, last_page


def dict_to_json(filename: str, data_array: list[dict], method="LOCAL"):
    # Open the JSON file for writing
    if method == "LOCAL":
        filename = f"./{filename}"
        with open(filename, 'w') as file:
            # Convert the list of dictionaries to JSON and save it to the file
            json.dump(data_array, file, indent=4)
    if method == "S3":
        # Convert the list of dictionaries to JSON
        json_str = json.dumps(data_array, indent=4)
        # Load the JSON string into an in-memory buffer
        json_buffer = StringIO(json_str)

        # Save the JSON data to a file in S3
        save_to_s3(BUCKET_NAME, filename, json_buffer, file_type="json")

        print("JSON file has been created.")


def dict_to_csv(filename: str, data_array: list[dict], method="LOCAL"):
    # Create a DictWriter object, specifying the fieldnames (column headers)
    if method == "LOCAL":
        filename = f"./{filename}"
        with open(filename, mode='w', newline='', encoding='utf-8') as file:
            fieldnames = []
            if len(data_array) == 0:
                pass

            fieldnames = data_array[0].keys()
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            # Write the header (column names)
            writer.writeheader()

            # Write the rows using the dictionaries
            for row in data_array:
                writer.writerow(row)

            print("CSV file has been created.")
    if method == "S3":
        # Create an in-memory string buffer
        csv_buffer = StringIO()

        # Create a csv writer object
        writer = csv.DictWriter(csv_buffer, fieldnames=data_array[0].keys())
        # Write the header and data to the buffer
        writer.writeheader()
        for row in data_array:
            writer.writerow(row)

        # Reset the buffer position to the beginning
        csv_buffer.seek(0)
        save_to_s3(BUCKET_NAME, filename, csv_buffer, file_type="csv")


def get_card_prices() -> list[dict]:
    card_price_array: list[dict[str, str | float | bool | None]] = []
    tcg_array_per_page, last_page = tcgcorner_scrape_per_page(1)
    card_price_array.extend(tcg_array_per_page)

    for page_number in range(2, last_page + 1):
        tcg_array_per_page, last_page = tcgcorner_scrape_per_page(page_number)
        card_price_array.extend(tcg_array_per_page)
    return card_price_array


def tcgcorner_scrape():
    card_prices = get_card_prices()

    csv_name = 'tcgcorner_pricing.csv'
    json_name = 'tcgcorner_pricing.json'
    dict_to_csv(csv_name, card_prices, "LOCAL")
    dict_to_json(json_name, card_prices, "LOCAL")
    dict_to_csv(csv_name, card_prices, "S3")
    dict_to_json(json_name, card_prices, "S3")
