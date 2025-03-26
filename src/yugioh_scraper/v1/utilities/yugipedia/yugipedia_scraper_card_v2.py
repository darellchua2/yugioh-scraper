import requests
from typing import List, Dict, Any, Optional
from ...models.yugipedia_models import YugiohCard

HEADERS: Dict[str, str] = {
    'authority': 'yugipedia.com',
    'User-Agent': 'yugioh card 1.0 - darellchua2@gmail.com',
    'From': 'darellchua2@gmail.com'
}


import json


# class YugiohCard:
#     """
#     A class to represent a Yu-Gi-Oh! card with relevant details.
#     """

#     def __init__(self, name: str, attributes: Dict[str, Any]) -> None:
#         self.name: str = name or ""
#         self.password: str = self.get_first(attributes.get("Password"))
#         self.card_type: str = self.extract_fulltext_single(
#             attributes.get("Card type"))
#         self.level: str = self.get_first(attributes.get("Level"))
#         self.race: str = self.extract_fulltext_single(
#             attributes.get("Primary type"))
#         self.type: str = self.extract_fulltext_single(attributes.get("Type"))
#         self.archetype_support = self.extract_fulltext_single(
#             attributes.get("Archetype support", ""))
#         self.archetypes: str = self.stringify_list(
#             self.extract_fulltext_list(attributes.get("Archseries")))
#         self.property: str = self.get_first(attributes.get("Property"))
#         self.lore: str = format_lore(self.get_first(attributes.get("Lore")))
#         self.attribute: str = self.extract_fulltext_single(
#             attributes.get("Attribute"))
#         self.atk_value: str = self.get_first(attributes.get("ATK"))
#         self.def_value: str = self.get_first(attributes.get("DEF"))
#         self.atk_string: str = self.get_first(attributes.get("ATK string"))
#         self.def_string: str = self.get_first(attributes.get("DEF string"))
#         self.link_arrows: str = self.stringify_list(
#             self.get_link_arrows(attributes.get("Link Arrows")))
#         self.link_rating: str = self.get_first(attributes.get("Link Rating"))
#         self.materials: str = self.get_first(attributes.get("Materials"))
#         self.pendulum_scale: str = self.get_first(
#             attributes.get("Pendulum Scale"))
#         self.pendulum_effect: str = format_lore(
#             self.get_first(attributes.get("Pendulum Effect")))
#         self.rank: str = self.get_first(attributes.get("Rank"))
#         self.ocg_status: str = self.extract_fulltext_single(
#             attributes.get("OCG status"))
#         self.card_image_name: str = self.get_first(
#             attributes.get("Card image name"))
#         self.release: str = self.extract_fulltext_single(
#             attributes.get("Release"))

#     def get_link_arrows(self, value: Optional[Any]) -> List[str]:
#         """
#         Processes the 'Link Arrows' attribute, returning a list of strings if present,
#         or an empty list if no link arrows are found.
#         """
#         if isinstance(value, list) and len(value) > 0:
#             print(value)
#             return [entry for entry in value]
#         return []

#     def get_first(self, value: Optional[Any]) -> str:
#         """
#         Helper method to extract the first value if the attribute is a list,
#         otherwise return the value as a string or an empty string.
#         """
#         if isinstance(value, list) and value:
#             return str(value[0])
#         return str(value) if value else ""

#     def extract_fulltext_single(self, value: Optional[Any]) -> str:
#         """
#         Extracts the 'fulltext' field from a single value if it exists.
#         Otherwise, returns an empty string.
#         """
#         if isinstance(value, list) and value and isinstance(value[0], dict) and "fulltext" in value[0]:
#             return value[0]["fulltext"]
#         return ""

#     def extract_fulltext_list(self, value: Optional[Any]) -> List[str]:
#         """
#         Extracts the 'fulltext' field from a list of values, returning a list of strings.
#         If the value is not a list or no 'fulltext' fields exist, returns an empty list.
#         """
#         if isinstance(value, list):
#             return [entry["fulltext"] for entry in value if isinstance(entry, dict) and "fulltext" in entry]
#         return []

#     def stringify_list(self, values: List[str]) -> str:
#         """
#         Converts a list of strings into a JSON-like stringified array.
#         """
#         return json.dumps(values)

#     def __str__(self) -> str:
#         """
#         Return a string representation of the card.
#         """
#         return (
#             f"Name: {self.name}\n"
#             f"Type: {self.card_type}\n"
#             f"Attribute: {self.attribute}\n"
#             f"ATK: {self.atk_value}\n"
#             f"DEF: {self.def_value}\n"
#             f"Archetypes: {self.archetypes}\n"
#             f"Link Arrows: {self.link_arrows}\n"
#             f"Lore: {self.lore}\n"
#         )


def card_semantic_search_params(character: str, offset: int, limit: int = 500) -> Dict[str, Any]:
    """
    Generate search parameters for Yugipedia semantic search API.
    """
    return {
        "q": f"[[Page type::Card page]][[Page name::~{character}*]][[Release::Yu-Gi-Oh! Official Card Game]]",
        "p": "format=json",
        "po": (
            "|?Password"
            "|?Card type"
            "|?Level"
            "|?Primary type"
            "|?Type"
            "|?Archetype support"
            "|?Property"
            "|?Lore"
            "|?Attribute"
            "|?ATK"
            "|?ATK string"
            "|?DEF string"
            "|?DEF"
            "|?Link Arrows"
            "|?Link Rating"
            "|?Materials"
            "|?Archseries"
            "|?Pendulum Scale"
            "|?Pendulum Effect"
            "|?Rank"
            "|?English name"
            "|?Page name"
            "|?OCG status"
            "|?Modification date"
            "|?Card image name"
            "|?Class 1"
            "|?Release"
        ),
        "title": "Special:Ask",
        "order": "asc",
        "offset": offset,
        "limit": limit,
        "eq": "yes",
        "link": "none"
    }


def fetch_card_data(character: str, offset: int = 0, limit: int = 500) -> Optional[Dict[str, Any]]:
    """
    Fetch card data from Yugipedia's API using semantic search.
    Handles errors gracefully to ensure the script continues.
    """
    base_url = "https://yugipedia.com/wiki/Special:Ask"
    params = card_semantic_search_params(character, offset, limit)

    try:
        response = requests.get(base_url, headers=HEADERS,
                                params=params, timeout=10)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx and 5xx)

        # Check if the response contains valid JSON
        try:
            return response.json()
        except ValueError as ve:
            print(f"Invalid JSON response for character '{character}': {ve}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data for '{character}': {e}")
        return None


def get_yugioh_cards_per_character(character: str, limit: int = 500) -> List[YugiohCard]:
    """
    Collect all card data for the given character by iterating through pages.
    """
    all_cards: List[YugiohCard] = []
    offset = 0

    while True:
        print(f"Fetching data with offset {offset}...")
        data = fetch_card_data(character, offset, limit)

        if data is None or "results" not in data:
            print("No more data found or an error occurred.")
            break

        results: dict[str, dict] = data.get("results", {})
        if not results:
            print("No additional results found.")
            break

        # Convert results into YugiohCard instances and add to the list
        for card_name, card_data in results.items():
            attributes = card_data.get("printouts", {})
            yugioh_card = YugiohCard(card_name, attributes)
            all_cards.append(yugioh_card)

        # Check if we have fetched all entries
        if len(results) < limit:
            print("All entries fetched.")
            break

        # Increment offset for next iteration
        offset += limit

    return all_cards


def display_card_data(cards: List[YugiohCard]) -> None:
    """
    Display card information in a readable format.
    """
    if not cards:
        print("No card data found.")
        return

    for card in cards:
        print(card)
        print("-" * 40)


import concurrent.futures
import string
import csv
from typing import List


def save_cards_to_csv(cards: List[YugiohCard], filename: str) -> None:
    """
    Save a list of YugiohCard instances to a CSV file.
    """
    with open(filename, mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)

        # Write header only if the file is empty
        if file.tell() == 0:
            writer.writerow([
                "name", "password", "card_type", "level", "race", "Type", "Archetype Support",
                "Property", "Lore", "Attribute", "atk_string", "def_string", "Link Arrows", "Link Rating", "Materials",
                "archtypes", "Pendulum Scale", "Pendulum Effect", "Rank", "OCG Status", "Card Image Name", "Release"
            ])

        # Write card data
        for card in cards:
            writer.writerow([
                card.name, card.password, card.card_type, card.level, card.race, card.type,
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
            print(
                f"Total cards found for '{search_character}': {len(all_cards)}")
            save_cards_to_csv(all_cards, output_file)
        else:
            print(f"No cards found for '{search_character}'.")
    except Exception as e:
        print(f"An unexpected error occurred for '{search_character}': {e}")


def main() -> None:
    """
    Main function to run the script.
    Iterates over all uppercase letters, digits, and specific special characters in a multi-threaded manner.
    """
    # Characters to iterate over: uppercase letters, digits, and specific special characters
    search_characters = list(string.ascii_uppercase) + \
        list(string.digits) + ["\"", "1", "3", "4", "7", "8", "@"]
    # search_characters = ['A']

    # Output CSV file
    output_file = "yugioh_cards.csv"

    # Multi-threaded execution
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(
            fetch_and_save_cards, char, output_file) for char in search_characters]
        concurrent.futures.wait(futures)

    print("\nAll tasks completed.")


if __name__ == "__main__":
    main()
