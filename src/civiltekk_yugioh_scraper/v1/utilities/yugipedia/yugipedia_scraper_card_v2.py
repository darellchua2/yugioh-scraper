import time
import requests
from typing import List, Dict, Any, Optional

from ...models.yugipedia_models import YugiohCard
from ..yugipedia.mediawiki_params import card_semantic_search_params, card_semantic_search_params_v2
from ...config import HEADERS
import concurrent.futures
import string
import csv
import requests
from typing import Optional, Dict, Any
import logging


def fetch_card_data_v2(character: str, offset: int = 0, limit: int = 500) -> Optional[Dict[str, Any]]:
    """
    Fetch card data from the Yugipedia MediaWiki API.
    Handles errors gracefully to ensure the script continues.
    """
    base_url = "https://yugipedia.com/api.php"
    params = card_semantic_search_params_v2(character, offset, limit)

    try:
        response = requests.get(base_url, params=params,
                                headers=HEADERS, timeout=60)
        # Raise HTTPError for bad responses (4xx and 5xx)
        response.raise_for_status()

        # Check if the response contains valid JSON
        try:
            return response.json()
        except ValueError as ve:
            logging.error(
                f"Invalid JSON response for character '{character}': {ve}")
            return None

    except requests.exceptions.RequestException as e:
        logging.error(
            f"An error occurred while fetching data for '{character}': {e}")
        return None


def get_yugioh_cards_per_character(character: str, limit: int = 500) -> List[YugiohCard]:
    """
    Collect all card data for the given character by iterating through pages.
    """
    all_cards: List[YugiohCard] = []
    offset = 0

    while True:
        logging.info(f"Fetching data with offset {offset}...")
        # time.sleep(0.5)
        data = fetch_card_data_v2(character, offset, limit)

        if data is None or "results" not in data:
            logging.info("No more data found or an error occurred.")
            break

        results: dict[str, dict] = data.get("results", {})
        if not results:
            logging.info("No additional results found.")
            break

        # Convert results into YugiohCard instances and add to the list
        for card_name, card_data in results.items():
            attributes = card_data.get("printouts", {})
            yugioh_card = YugiohCard(card_name, attributes)
            all_cards.append(yugioh_card)

        # Check if we have fetched all entries
        if len(results) < limit:
            logging.info("All entries fetched.")
            break

        # Increment offset for next iteration
        offset += limit

    return all_cards


def save_cards_to_csv(cards: List[YugiohCard], filename: str) -> None:
    """
    Save a list of YugiohCard instances to a CSV file.
    """
    with open(filename, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # Write header only if the file is empty
        if file.tell() == 0:
            writer.writerow([
                "name", "english_name", "password", "card_type", "level", "race", "Type", "Archetype Support",
                "Property", "Lore", "Attribute", "atk_string", "def_string", "Link Arrows", "Link Rating", "Materials",
                "archtypes", "Pendulum Scale", "Pendulum Effect", "Rank", "OCG Status", "Card Image Name", "Release"
            ])

        # Write card data
        for card in cards:
            writer.writerow([
                card.name, card.english_name, card.password, card.card_type, card.level, card.race, card.type,
                card.archetype_support, card.property, card.lore, card.attribute, card.atk_string, card.def_string,
                card.link_arrows, card.link_rating, card.materials, card.archetypes, card.pendulum_scale,
                card.pendulum_effect, card.rank, card.ocg_status, card.card_image_name, card.release
            ])


def fetch_and_save_cards(search_character: str, output_file: str) -> None:
    """
    Fetch card data for a specific search character and save to a CSV file.
    Gracefully handles cases where no data is found or an error occurs.
    """
    try:
        print(f"\nFetching data for character: {search_character}...")
        all_cards = get_yugioh_cards_per_character(search_character)

        if all_cards:
            logging.info(
                f"Total cards found for '{search_character}': {len(all_cards)}")
            save_cards_to_csv(all_cards, output_file)
        else:
            logging.info(f"No cards found for '{search_character}'.")
    except Exception as e:
        logging.error(
            f"An unexpected error occurred for '{search_character}': {e}")


def get_card_data(character: str, offset: int = 0, limit: int = 500) -> Optional[Dict[str, Any]]:
    """
    Fetch card data from Yugipedia's API using semantic search.
    Handles errors gracefully to ensure the script continues.
    """
    base_url = "https://yugipedia.com/wiki/Special:Ask"
    params = card_semantic_search_params(character, offset, limit)

    try:
        time.sleep(1)
        response = requests.get(base_url, headers=HEADERS,
                                params=params, timeout=60)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)

        # Check if the response contains valid JSON
        try:
            return response.json()
        except ValueError as ve:
            logging.error(
                f"Invalid JSON response for character '{character}': {ve}")
            return None

    except requests.exceptions.RequestException as e:
        logging.error(
            f"An error occurred while fetching data for '{character}': {e}")
        return None


def get_yugioh_cards_per_semantic_card_search_per_character_v2(character: str, limit: int = 500) -> List[YugiohCard]:
    """
    Collect all card data for the given character by iterating through pages.
    """
    all_cards: List[YugiohCard] = []
    offset = 0
    while True:
        print(
            f"Fetching data with offset {offset} for character '{character}'...")
        time.sleep(3)
        data = get_card_data(
            character, offset, limit)

        if data is None or "results" not in data:
            logging.info("No more data found or an error occurred.")
            break

        results: dict[str, dict] = data.get("results", {})
        if not results:
            logging.info("No additional results found.")
            break

        # Convert results into YugiohCard instances and add to the list
        for card_name, card_data in results.items():
            attributes = card_data.get("printouts", {})
            yugioh_card = YugiohCard(card_name, attributes)
            all_cards.append(yugioh_card)

        # Check if we have fetched all entries
        if len(results) < limit:
            logging.info("All entries fetched.")
            break

        # Increment offset for next iteration
        offset += limit

    return all_cards


def get_yugioh_cards() -> list[YugiohCard]:
    character_list = list(string.ascii_uppercase)
    character_list.extend(
        ["\"", "1", "3", "4", "7", "8", "@"])
    yugioh_cards: list[YugiohCard] = []

    # optimally defined number of threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for character in character_list:
            time.sleep(1)
            futures.append(executor.submit(
                get_yugioh_cards_per_semantic_card_search_per_character_v2, character))
        for future in concurrent.futures.as_completed(futures):
            try:
                yugioh_cards.extend(future.result().copy())
            except requests.exceptions.JSONDecodeError as e:
                pass
        logging.info("Total semantic cards:{overall_list_count}".format(
            overall_list_count=len(yugioh_cards)))

    return yugioh_cards


def main() -> None:
    """
    Main function to run the script.
    Iterates over all uppercase letters, digits, and specific special characters in a multi-threaded manner.
    """
    # Characters to iterate over: uppercase letters, digits, and specific special characters
    search_characters = list(string.ascii_uppercase) + \
        list(string.digits) + ["\"", "1", "3", "4", "7", "8", "@", "#"]

    # Output CSV file
    output_file = "yugioh_cards.csv"

    # Multi-threaded execution
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(
            fetch_and_save_cards, char, output_file) for char in search_characters]
        concurrent.futures.wait(futures)

    logging.info("\nAll tasks completed.")


if __name__ == "__main__":
    main()
