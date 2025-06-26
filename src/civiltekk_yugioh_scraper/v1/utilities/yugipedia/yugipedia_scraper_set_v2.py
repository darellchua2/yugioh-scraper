import datetime
import re
import requests
import csv
from typing import Dict, Any, List, Optional, Union
import time
import logging
import pandas as pd
import os
from ...config import HEADERS, MEDIAWIKI_URL, SEMANTIC_URL, TABLE_YUGIOH_SETS

from ..aws_utilities import upload_data

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s")


def clean_set_name(name: str, region: str, is_gallery: bool = False) -> str:
    patterns: Dict[str, str] = {
        "Asian-English": r"^Set Card (?:Lists|Galleries):| \(OCG-AE(?:-UE|-1E|-LE)?\)",
        "Japanese": r"^Set Card (?:Lists|Galleries):| \((?:OCG|DM)-JP(?:-Reprint)?\)",
        "Japanese-Asian": r"^Set Card (?:Lists|Galleries):| \(OCG-JA\)"
    }
    pattern: str = patterns.get(region, "")
    return re.sub(pattern, "", name)


class YugiohSet:
    def __init__(
        self,
        region: str,
        set_card_list_page_id: str,
        set_card_list_name: Optional[str] = None,
        set_card_gallery_name: Optional[str] = None,
        set_card_gallery_page_id: Optional[str] = None
    ):
        if set_card_gallery_name is None and set_card_list_name is None:
            raise ValueError(
                "Please provide at least 'set_card_list_name' or 'set_card_gallery_name'")

        self.yugipedia_set_card_list: str = set_card_list_name if set_card_list_name else ""
        self.yugipedia_set_card_gallery: str = set_card_gallery_name if set_card_gallery_name else ""
        self.region: str = region

        language: str = ""
        if region == "Japanese":
            language = "JP"
        if region == "Japanese-Asian":
            language = "JA"
        if region == "Asian-English":
            language = "AE"
        self.language: str = language

        set_name: str = ""
        if set_card_list_name:
            set_name = clean_set_name(set_card_list_name, region)
            logging.info(f"region: {region} | set_name: {set_name}")
            self.yugipedia_set_card_list_url: str = f"https://yugipedia.com/wiki/{set_card_list_name}"
            self.set_card_list_page_id: str = set_card_list_page_id

        if set_card_gallery_name:
            set_name = clean_set_name(
                set_card_gallery_name, region, is_gallery=True)
            self.yugipedia_set_card_gallery_url: str = f"https://yugipedia.com/wiki/{set_card_gallery_name}"
            self.set_card_gallery_page_id: str = set_card_gallery_page_id if set_card_gallery_page_id else ""

        if not set_name:
            raise ValueError("Invalid set name")

        self.name: str = set_name
        self.set_type: str = ""
        self.series: str = ""
        self.prefix: str = ""
        self.set_image: str = ""
        self.release_date: str = ""
        self.image_file: str = ""
        self.image_url: str = ""
        self.card_game: str = "OCG"
        self.yugipedia_set_image_file: str = ""
        self.set_code: str = ""

    # @property
    # def set_code(self) -> str:
    #     """
    #     Extracts the main set code from the prefix property by removing regional suffixes.
    #     Examples:
    #     - AGOV-AE -> AGOV
    #     - AGOV-JP -> AGOV
    #     - XYZ-EN -> XYZ
    #     """
    #     if not self.prefix:
    #         return ""
    #     return re.sub(r"-[A-Za-z]+$", "", self.prefix)

    def get_dict(self):
        return self.__dict__


def assign_set_code(prefix: str):
    """
    Extracts the main set code from the prefix property by removing regional suffixes.
    Examples:
    - AGOV-AE -> AGOV
    - AGOV-JP -> AGOV
    - XYZ-EN -> XYZ
    """
    if not prefix:
        return ""
    return re.sub(r"-[A-Za-z]+$", "", prefix)


def fetch_json_with_generator(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    combined_results: List[Dict[str, Any]] = []
    retries: int = 3

    while retries > 0:
        try:
            response = requests.get(
                url, headers=HEADERS, params=params, timeout=10)
            response.raise_for_status()
            json_data: Dict[str, Any] = response.json()
            combined_results.append(json_data)

            if 'continue' in json_data:
                params.update(json_data['continue'])
            else:
                break
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            retries -= 1
            if retries == 0:
                raise Exception(
                    f"Failed to fetch data after multiple retries: {e}")
            print(f"Retrying... ({3 - retries}/3)")
            time.sleep(1)

    return combined_results


def step_1_fetch_all_set_card_lists_for_regions(api_url: str, regions: List[str]) -> List[YugiohSet]:
    yugioh_sets: List[YugiohSet] = []

    for region in regions:
        category_set_card_list_name: str = f"{region} Set Card Lists"
        set_card_lists_params: Dict[str, Any] = {
            'action': 'query',
            'format': 'json',
            'generator': 'categorymembers',
            'gcmtitle': f"Category:{category_set_card_list_name}",
            'gcmlimit': 500,
        }

        set_card_list_results: List[Dict[str, Any]] = fetch_json_with_generator(
            api_url, set_card_lists_params)

        for result in set_card_list_results:
            pages: Dict[str, Any] = result.get('query', {}).get('pages', {})
            for page_id, page_data in pages.items():
                try:
                    if not page_data['title'].startswith("Set Card Lists:"):
                        continue
                    yugioh_sets.append(
                        YugiohSet(
                            set_card_list_name=page_data['title'],
                            region=region,
                            set_card_list_page_id=page_id
                        )
                    )
                except Exception as e:
                    logging.warning(
                        f"Error with page {page_data['title']}: {e}")
                    continue

    return yugioh_sets


def step_2_fetch_all_set_card_galleries_for_regions(
    api_url: str, regions: List[str], existing_yugioh_sets: List[YugiohSet]
) -> List[YugiohSet]:
    for region in regions:
        category_set_card_galleries_name: str = f"{region} Set Card Galleries"

        set_card_galleries_params: Dict[str, Any] = {
            'action': 'query',
            'format': 'json',
            'generator': 'categorymembers',
            'gcmtitle': f"Category:{category_set_card_galleries_name}",
            'gcmlimit': 500,
        }

        set_card_gallery_results: List[Dict[str, Any]] = fetch_json_with_generator(
            api_url, set_card_galleries_params)

        for result in set_card_gallery_results:
            pages: Dict[str, Any] = result.get('query', {}).get('pages', {})
            for page_id, page_data in pages.items():
                set_gallery_name: str = page_data['title']
                set_name: str = ""
                language: str | None = None
                match region:
                    case "Asian-English":
                        set_name = re.sub(
                            r"^Set Card Galleries:| \(OCG-AE(?:-UE|-1E|-LE)?\)", "", set_gallery_name)
                        language = "AE"
                    case "Japanese":
                        set_name = re.sub(
                            r"^Set Card Galleries:| \((?:OCG|DM)-JP(?:-Reprint)?\)", "", set_gallery_name)
                        language = "JP"
                    case "Japanese-Asian":
                        set_name = re.sub(
                            r"^Set Card Galleries:| \(OCG-JA\)", "", set_gallery_name)
                        language = "JA"
                    case _:
                        set_name = ""
                        logging.warning(f"Unhandled region '{region}'")

                matching_set: Optional[YugiohSet] = next(
                    (
                        yugioh_set for yugioh_set in existing_yugioh_sets
                        if yugioh_set.name == set_name and yugioh_set.region == region
                    ),
                    None
                )

                if matching_set:
                    matching_set.yugipedia_set_card_gallery = set_gallery_name
                    matching_set.set_card_gallery_page_id = page_id
                else:
                    if not set_gallery_name.startswith("Set Card Galleries:") or set_gallery_name.endswith("Reprint)"):
                        continue
                    existing_yugioh_sets.append(
                        YugiohSet(
                            set_card_gallery_name=set_gallery_name,
                            region=region,
                            set_card_list_page_id=page_id
                        )
                    )

    return existing_yugioh_sets


def save_sets_to_csv(yugioh_sets: List[YugiohSet], filename: str) -> None:
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['name', 'region', "prefix", "set_code", "set_type", "release_date",
                         "set_card_list_name", "set_card_gallery_name", "image_url"])
        for yugioh_set in yugioh_sets:
            writer.writerow([
                yugioh_set.name,
                yugioh_set.region,
                yugioh_set.prefix,
                yugioh_set.set_code,  # Include set_code here
                yugioh_set.set_type,
                yugioh_set.release_date,
                yugioh_set.yugipedia_set_card_list,
                yugioh_set.yugipedia_set_card_gallery,
                yugioh_set.image_url
            ])


def fetch_results(base_url: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        response = requests.get(base_url, headers=HEADERS,
                                params=params, timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching results: {e}")
        return None


def yugioh_set_semantic_search_params(offset: int, limit: int = 500) -> Dict[str, Union[str, int]]:
    return {
        "q": "[[Page type::Set page]][[Medium::OCG]][[Medium::Official]][[Japanese release date::+]]",
        "p": "format=json",
        "po": "\
            |?Page name\
            |?Set image\
            |?Series\
            |?Set type\
            |?Asian-English set and region prefix\
            |?Japanese set and region prefix\
            |?Japanese-Asian set and region prefix\
            |?Asian-English release date\
            |?Japanese release date\
            |?Japanese-Asian release date",
        "title": "Special:Ask",
        "order": "asc",
        "offset": offset,
        "limit": limit,
        "eq": "yes",
        "link": "none"
    }


def yugioh_set_semantic_search_params_to_remove_rush_duel(offset: int, limit: int = 500) -> Dict[str, Union[str, int]]:
    return {
        "q": "[[Page type::Set page]][[Medium::Yu-Gi-Oh! Rush Duel]]",
        "p": "format=json",
        "po": "\
            |?Page name\
            |?Set image\
            |?Series\
            |?Set type\
            |?Asian-English set and region prefix\
            |?Japanese set and region prefix\
            |?Japanese-Asian set and region prefix\
            |?Asian-English release date\
            |?Japanese release date\
            |?Japanese-Asian release date",
        "title": "Special:Ask",
        "order": "asc",
        "offset": offset,
        "limit": limit,
        "eq": "yes",
        "link": "none"
    }


def fetch_yugioh_set_semantic_results(base_url: str = SEMANTIC_URL, limit: int = 500) -> List[Dict[str, Any]]:
    all_results: List[Dict[str, Any]] = []
    offset: int = 0

    while True:
        params: Dict[str, Any] = yugioh_set_semantic_search_params(
            offset, limit)
        results: Optional[Dict[str, Any]] = fetch_results(base_url, params)

        if not results or "results" not in results:
            logging.info("No more results found.")
            break

        for result_key, result_value in results["results"].items():
            all_results.append(result_value.get('printouts', {}))

        offset += limit
        logging.info(
            f"Fetched {len(results.get('results', {}))} results. Total so far: {len(all_results)}")
        time.sleep(1)

    return all_results


def update_yugioh_sets_with_semantic_results(yugioh_sets: List[YugiohSet], objs: List[Dict[str, Any]]) -> List[YugiohSet]:
    for obj in objs:
        if not isinstance(obj, dict):
            logging.warning(f"Unexpected data type: {type(obj)}")
            continue

        set_name: Optional[str] = obj.get("Page name", [None])[0]
        if not set_name:
            logging.info(f"Skipping result with missing 'Page name': {obj}")
            continue

        matching_set: Optional[YugiohSet] = next(
            (y_set for y_set in yugioh_sets if y_set.name == set_name),
            None
        )
        matching_sets = [
            y_set for y_set in yugioh_sets if y_set.name == set_name]

        if set_name == "Age of Overlord":
            print("set_name: Age of Overlord")
            print("matching_set: ", matching_set.__dict__)
            print("matching sets: ", [
                  matching_set.__dict__ for matching_set in matching_sets])

        if matching_set:
            matching_set.set_type = obj.get("Set type", [""])[0].get("fulltext", "") if len(obj.get(
                "Set type", [])) > 0 else ""
            matching_set.series = obj.get("Series", [""])[0].get("fulltext", "") if len(obj.get(
                "Series", [])) > 0 else ""
            matching_set.set_image = obj.get("Set image", [""])[0] if len(obj.get(
                "Set image", [])) > 0 else ""
            matching_set.image_file = "File:{set_image}".format(
                set_image=obj.get("Set image", [""])[0]) if len(obj.get(
                    "Set image", [])) > 0 else ""
            if matching_set.region == "Japanese":
                matching_set.prefix = obj.get(
                    "Japanese set and region prefix", [""])[0] if len(obj.get(
                        "Japanese set and region prefix", [])) > 0 else ""
                matching_set.release_date = datetime.datetime.fromtimestamp(int(obj.get(
                    "Japanese release date", [""])[0]["timestamp"])).strftime('%Y-%m-%d') if len(obj.get(
                        "Japanese release date", [])) > 0 else ""
            elif matching_set.region == "Asian-English":
                matching_set.prefix = obj.get(
                    "Asian-English set and region prefix", [""])[0] if len(obj.get(
                        "Asian-English set and region prefix", [])) > 0 else ""
                matching_set.release_date = datetime.datetime.fromtimestamp(int(obj.get(
                    "Asian-English release date", [""])[0]["timestamp"])).strftime('%Y-%m-%d') if len(obj.get(
                        "Asian-English release date", [])) > 0 else ""
            elif matching_set.region == "Japanese-Asian":
                matching_set.prefix = obj.get(
                    "Japanese-Asian set and region prefix", [""])[0] if len(obj.get(
                        "Japanese-Asian set and region prefix", [])) > 0 else ""
                matching_set.release_date = datetime.datetime.fromtimestamp(int(obj.get(
                    "Japanese-Asian release date", [""])[0]["timestamp"])).strftime('%Y-%m-%d') if len(obj.get(
                        "Japanese-Asian release date", [])) > 0 else ""

            matching_set.set_code = assign_set_code(matching_set.prefix)

    return yugioh_sets


def update_yugioh_sets_with_semantic_results_v2(yugioh_sets: List[YugiohSet], objs: List[Dict[str, Any]]) -> List[YugiohSet]:
    for obj in objs:
        if not isinstance(obj, dict):
            logging.warning(f"Unexpected data type: {type(obj)}")
            continue

        set_name: Optional[str] = obj.get("Page name", [None])[0]
        if not set_name:
            logging.info(f"Skipping result with missing 'Page name': {obj}")
            continue

        matching_sets = [
            y_set for y_set in yugioh_sets if y_set.name == set_name]

        # if set_name == "Age of Overlord":
        #     print("set_name: Age of Overlord")
        #     print("matching_set: ", matching_set.__dict__)
        #     print("matching sets: ", [
        #           matching_set.__dict__ for matching_set in matching_sets])
        for matching_set in matching_sets:
            matching_set.set_type = obj.get("Set type", [""])[0].get("fulltext", "") if len(obj.get(
                "Set type", [])) > 0 else ""
            matching_set.series = obj.get("Series", [""])[0].get("fulltext", "") if len(obj.get(
                "Series", [])) > 0 else ""
            matching_set.set_image = obj.get("Set image", [""])[0] if len(obj.get(
                "Set image", [])) > 0 else ""
            matching_set.image_file = "File:{set_image}".format(
                set_image=obj.get("Set image", [""])[0]) if len(obj.get(
                    "Set image", [])) > 0 else ""
            if matching_set.region == "Japanese":
                matching_set.prefix = obj.get(
                    "Japanese set and region prefix", [""])[0] if len(obj.get(
                        "Japanese set and region prefix", [])) > 0 else ""
                matching_set.release_date = datetime.datetime.fromtimestamp(int(obj.get(
                    "Japanese release date", [""])[0]["timestamp"])).strftime('%Y-%m-%d') if len(obj.get(
                        "Japanese release date", [])) > 0 else ""
            elif matching_set.region == "Asian-English":
                matching_set.prefix = obj.get(
                    "Asian-English set and region prefix", [""])[0] if len(obj.get(
                        "Asian-English set and region prefix", [])) > 0 else ""
                matching_set.release_date = datetime.datetime.fromtimestamp(int(obj.get(
                    "Asian-English release date", [""])[0]["timestamp"])).strftime('%Y-%m-%d') if len(obj.get(
                        "Asian-English release date", [])) > 0 else ""
            elif matching_set.region == "Japanese-Asian":
                matching_set.prefix = obj.get(
                    "Japanese-Asian set and region prefix", [""])[0] if len(obj.get(
                        "Japanese-Asian set and region prefix", [])) > 0 else ""
                matching_set.release_date = datetime.datetime.fromtimestamp(int(obj.get(
                    "Japanese-Asian release date", [""])[0]["timestamp"])).strftime('%Y-%m-%d') if len(obj.get(
                        "Japanese-Asian release date", [])) > 0 else ""

            matching_set.set_code = assign_set_code(matching_set.prefix)

    return yugioh_sets


def fetch_rush_duel_set_names(base_url: str = SEMANTIC_URL, limit: int = 500) -> List[str]:
    """
    Fetch the list of set names for Rush Duel sets to exclude.

    Args:
        base_url (str): The base URL for the API endpoint. Defaults to SEMANTIC_URL.
        limit (int): The maximum number of results to fetch per API call. Defaults to 500.

    Returns:
        List[str]: A list of set names to exclude.
    """
    rush_duel_set_names: List[str] = []
    offset: int = 0

    while True:
        params: Dict[str, Any] = yugioh_set_semantic_search_params_to_remove_rush_duel(
            offset, limit)
        results: Optional[Dict[str, Any]] = fetch_results(base_url, params)

        if not results or "results" not in results:
            logging.info("No more Rush Duel results found.")
            break

        for result_key, result_value in results["results"].items():
            set_name = result_value.get(
                'printouts', {}).get("Page name", [None])[0]
            if set_name:
                rush_duel_set_names.append(set_name)

        offset += limit
        logging.info(
            f"Fetched {len(results.get('results', {}))} Rush Duel results. Total so far: {len(rush_duel_set_names)}")
        time.sleep(1)

    return rush_duel_set_names


def remove_rush_duel_sets(yugioh_sets: List[YugiohSet], rush_duel_set_names: List[str]) -> List[YugiohSet]:
    """
    Remove rows from yugioh_sets that have set names appearing in rush_duel_set_names.

    Args:
        yugioh_sets (List[YugiohSet]): The list of YugiohSet objects.
        rush_duel_set_names (List[str]): The list of set names to exclude.

    Returns:
        List[YugiohSet]: The filtered list of YugiohSet objects.
    """
    filtered_sets = [
        yugioh_set for yugioh_set in yugioh_sets if yugioh_set.name not in rush_duel_set_names
    ]
    logging.info(
        f"Filtered out {len(yugioh_sets) - len(filtered_sets)} Rush Duel sets.")
    return filtered_sets


def get_image_links_from_image_file_mediawiki_params(image_card_urls: list[str]) -> dict:
    image_card_url_list_string = "|".join(image_card_urls)
    obj = {
        "action": "query",
        "format": "json",
        "prop": "links",
        "titles": image_card_url_list_string,
        "pilimit": "500",
        "plnamespace": "0"
    }
    return obj


def get_yugioh_set_card_image_urls(image_files: List[str]) -> List[Dict[str, str]]:
    """
    Fetch image URLs for up to 500 image files using MediaWiki's API using the pageimage property.

    Args:
        image_files (List[str]): A list of image file names to fetch URLs for.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing 'image_file' and 'image_url' keys.
    """
    image_file_obj_list: List[Dict[str, str]] = []
    batch_size = 50  # MediaWiki's API limit for non-bots is 50
    batches = [image_files[i:i + batch_size]
               for i in range(0, len(image_files), batch_size)]

    for batch in batches:
        params: Dict[str, Union[str, int]] = {
            "action": "query",
            "format": "json",
            "prop": "pageimages",
            "titles": "|".join([f"{image_file}" for image_file in batch]),
            "piprop": "original",  # Fetch the original image URL
            "pilimit": "500",
            "plnamespace": "0"
        }

        try:
            # Make the API request
            response = requests.get(
                MEDIAWIKI_URL, headers=HEADERS, params=params, timeout=10)
            response.raise_for_status()
            res_json: Dict[str, Any] = response.json()

            # Parse response to extract image URLs
            if "query" in res_json and "pages" in res_json["query"]:
                for page in res_json["query"]["pages"].values():
                    if "original" in page:
                        image_file_obj_list.append({
                            "image_file": page["title"],
                            "image_url": page["original"]["source"]
                        })
        except Exception as e:
            logging.error(
                f"Error fetching image URLs for batch: {batch}. Error: {e}")

    return image_file_obj_list


def get_yugioh_sets_v2(api_url: str = MEDIAWIKI_URL, regions: List[str] = ['Japanese', 'Asian-English', 'Japanese-Asian'], output_filename: str = 'yugioh_sets.csv') -> List[YugiohSet]:
    # Step 1: Fetch all sets
    yugioh_sets: List[YugiohSet] = step_1_fetch_all_set_card_lists_for_regions(
        api_url, regions)
    yugioh_sets = step_2_fetch_all_set_card_galleries_for_regions(
        api_url, regions, yugioh_sets)

    # Step 2: Fetch semantic results and update sets
    semantic_results: List[Dict[str, Any]
                           ] = fetch_yugioh_set_semantic_results()
    yugioh_sets = update_yugioh_sets_with_semantic_results_v2(
        yugioh_sets, semantic_results)

    # Step 3: Fetch Rush Duel set names to exclude
    rush_duel_set_names: List[str] = fetch_rush_duel_set_names()

    # Step 4: Remove Rush Duel sets
    yugioh_sets = remove_rush_duel_sets(yugioh_sets, rush_duel_set_names)

    # Step 5: Fetch image URLs for the image_file properties
    image_files: List[str] = [
        yugioh_set.image_file for yugioh_set in yugioh_sets if yugioh_set.image_file]
    if image_files:
        image_urls = get_yugioh_set_card_image_urls(image_files)

        # Map the fetched image URLs back to their respective YugiohSet objects
        for yugioh_set in yugioh_sets:
            image_url_data = next(
                (img for img in image_urls if img["image_file"] == yugioh_set.image_file), None)
            if image_url_data:
                yugioh_set.image_url = image_url_data["image_url"]

    # Step 6: Save filtered sets to CSV
    save_sets_to_csv(yugioh_sets, output_filename)
    logging.info(f"Set titles saved to {output_filename}")

    return yugioh_sets


# Main execution
if __name__ == "__main__":
    # Get the current project path
    project_path = os.getcwd()

    # Define the output folder relative to the project path
    output_folder = os.path.join(project_path, "output")

    # regions: List[str] = ['Japanese', 'Asian-English', 'Japanese-Asian']
    yugioh_sets = get_yugioh_sets_v2()
    db_data = [yugioh_set.get_dict() for yugioh_set in yugioh_sets]
    df = pd.DataFrame(db_data)
    df.to_csv(os.path.join(output_folder, "yugioh_sets.csv"), index=False)

    upload_data(df, table_name=TABLE_YUGIOH_SETS,
                if_exist="replace", db_name='yugioh_data')
