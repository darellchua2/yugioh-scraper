import requests
import json


class TcgCornerScraper:
    def __init__(self) -> None:
        pass

    def scrape(self, url: str):
        try:
            print(f"Scraping URL: {url}")
            # Send a GET request to the URL
            response = requests.get(url)

            # Raise an error if the request was not successful
            response.raise_for_status()

            # Parse the JSON data
            data = response.json()

            # Assuming the data has a structure containing items/products
            products = data.get('products', [])
            print(f"Found {len(products)} products.")

            # Return or process the products as needed
            return products

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while scraping: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON: {e}")
            return None


class TcgCornerRarity:
    def __init__(self) -> None:
        pass

    def get_list_of_website_rarity(self):
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
        return rarity_dict
