import os
import time

from .set_card_list_scraper import get_yugioh_set_cards_from_set_card_list_names

from ..utilities.yugipedia.yugipedia_scraper_rarity_v2 import get_yugioh_rarities_v2

from .yugipedia.yugipedia_scraper_set_v2 import get_yugioh_sets_v2

from .misc_utilities import run_wiki_request_until_response
from .aws_utilities import retrieve_data_from_db_to_df, upload_data
import requests
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import string
from ..models.yugipedia_models import YugiohCard, YugiohSet, YugiohRarity, YugiohSetCard
from typing import Any, Dict, List, Optional
from ..config import MEDIAWIKI_URL, HEADERS, TABLE_YUGIOH_CARDS, TABLE_YUGIOH_SETS, TABLE_YUGIOH_RARITIES


def card_semantic_search_params(character, offset, limit=500) -> dict:
    obj = {
        "q": "[[Page type::Card page]][[Page name::~{character}*]][[Release::Yu-Gi-Oh! Official Card Game]]".format(character=str(character)),
        "p": "format=json",
        "po":
        "\
            |?Password\
            |?Card type\
            |?Level\
            |?Primary type\
            |?Type\
            |?Archetype support\
            |?Property\
            |?Lore\
            |?Attribute\
            |?ATK\
            |?ATK string\
            |?DEF string\
            |?DEF\
            |?Link Arrows\
            |?Link Rating\
            |?Materials\
            |?Archseries\
            |?Pendulum Scale\
            |?Pendulum Effect\
            |?Rank\
            |?English name\
            |?Page name\
            |?OCG status\
            |?Modification date\
            |?Card image name\
            |?Class 1\
            |?Release\
        ",
        "title": "Special:Ask",
        "order": "asc",
        "offset": offset,
        "limit": limit,
        "eq": "yes",
        "link": "none"
    }

    return obj


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


def get_yugioh_cards() -> list[YugiohCard]:
    character_list = list(string.ascii_uppercase)
    character_list.extend(
        ["\"", "1", "3", "4", "7", "8", "@"])
    yugioh_cards: list[YugiohCard] = []

    with ThreadPoolExecutor() as executor:  # optimally defined number of threads
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
        print("Total semantic cards:{overall_list_count}".format(
            overall_list_count=len(yugioh_cards)))

    return yugioh_cards


def get_split_data_from_image_file_v2(image_file: str) -> tuple[str | None, str | None, str | None, str | None, str | None, str | None, bool]:
    pat1 = re.compile(
        r"^File:(.[^\-]+)-(.[^\-]+)-(.[^\-]+)-(.[^\-]*)-?(.[^\-]*)?(.png|.jpg|.jpeg|.gif)$")
    pat_match = pat1.match(image_file)
    if pat_match:
        yugioh_card_image_name = pat_match.group(1)
        yugioh_set_code = pat_match.group(2)
        yugioh_set_language = pat_match.group(3)
        yugioh_rarity_code = pat_match.group(
            4)
        alternate_art_code = pat_match.group(
            5)
        file_extension = pat_match.group(6)
        is_alternate_art = False

        if alternate_art_code in ["AA", "Alt"]:
            is_alternate_art = True
        return yugioh_card_image_name, yugioh_set_code, yugioh_set_language, yugioh_rarity_code, alternate_art_code, file_extension, is_alternate_art
    else:
        return None, None, None, None, None, None, False


def is_image_file_yugioh_set_card(image_file) -> tuple[bool, bool]:
    pat1 = re.compile(
        r"^File:(.[^\-]+)-(.[^\-]+)-(.[^\-]+)-(.[^\-]*)-?(.[^\-]*)?(.png|.jpg|.jpeg|.gif)$")
    pat_match = pat1.match(image_file)
    if pat_match:
        is_offcial_proxy: bool = True if pat_match.group(4) == "OP" else False
        return True, is_offcial_proxy
    return False, False


def split(list_a, chunk_size):

    for i in range(0, len(list_a), chunk_size):
        yield list_a[i:i + chunk_size]


def get_set_card_gallery_mediawiki_params(set_card_galleries: list[str]) -> dict[str, str]:
    set_card_gallery_list_string = "|".join(set_card_galleries)
    obj = {
        "action": "query",
        "format": "json",
        "prop": "images",
        "titles": set_card_gallery_list_string,
        "imlimit": "500"
    }
    return obj


def get_set_card_list_links_mediawiki_params(set_card_galleries: list[str]) -> dict:
    set_card_galleries_string = "|".join(set_card_galleries)
    obj = {
        "action": "query",
        "format": "json",
        "prop": "links",
        "titles": set_card_galleries_string,
        "plnamespace": "0",
        "pllimit": "5000",
    }
    return obj


def get_page_images_from_image_file_mediawiki_params(image_card_urls: list[str]) -> dict:
    image_card_url_list_string = "|".join(image_card_urls)
    obj = {
        "action": "query",
        "format": "json",
        "prop": "pageimages",
        "titles": image_card_url_list_string,
        "pilimit": "50",
        "piprop": "name|original"
    }
    return obj


def get_yugioh_set_card_image_file_v2(split_yugioh_sets: List[YugiohSet],
                                      yugioh_sets: List[YugiohSet],
                                      yugioh_cards: List[YugiohCard],
                                      yugioh_rarities: List[YugiohRarity]
                                      ) -> tuple[list[dict[str, str | YugiohSet]], List[YugiohSetCard]]:
    """_summary_

    Parameters
    ----------
    obj : dict
        _description_

    Returns
    -------
    list[dict[str, str]]
        _description_
    """

    yugioh_set_card_image_file_list: list[dict] = []
    yugioh_set_cards: list[YugiohSetCard] = []
    card_list_obj_params = get_set_card_gallery_mediawiki_params(
        [ygo_set.yugipedia_set_card_gallery for ygo_set in split_yugioh_sets if isinstance(ygo_set, YugiohSet)])
    set_dict = {
        yugioh_set.yugipedia_set_card_gallery: yugioh_set for yugioh_set in yugioh_sets}
    try:
        res_json = run_wiki_request_until_response(
            MEDIAWIKI_URL, HEADERS, card_list_obj_params)
        if res_json:
            res_json_query_obj: dict[str, dict] = res_json["query"]["pages"]
            for set_card_gallery_page in res_json_query_obj.values():
                yugioh_set_found: YugiohSet | None = set_dict.get(
                    set_card_gallery_page['title'], None)
                if isinstance(yugioh_set_found, YugiohSet):
                    if "images" in set_card_gallery_page.keys():
                        for image_file_item in set_card_gallery_page["images"]:
                            (check_is_yugioh_set_card, is_official_proxy) = is_image_file_yugioh_set_card(
                                image_file_item["title"])
                            if check_is_yugioh_set_card and not is_official_proxy:
                                yugioh_rarity_found = None
                                yugioh_card_found = None
                                yugioh_card_image_name, yugioh_set_code, yugioh_set_language, yugioh_rarity_code, alternate_art_code, file_extension, is_alternate_art = get_split_data_from_image_file_v2(
                                    image_file=image_file_item["title"])

                                yugioh_card_found = next(
                                    (ygo_card for ygo_card in yugioh_cards if ygo_card.card_image_name == yugioh_card_image_name), yugioh_card_found)
                                yugioh_rarity_found = next(
                                    (ygo_card for ygo_card in yugioh_rarities if ygo_card.prefix == yugioh_rarity_code), yugioh_rarity_found)

                                list_obj = {}
                                list_obj["image_file"] = image_file_item["title"]
                                list_obj["yugioh_set"] = yugioh_set_found
                                yugioh_set_card = YugiohSetCard(
                                    yugioh_set=yugioh_set_found,
                                    yugioh_card=yugioh_card_found,
                                    yugioh_rarity=yugioh_rarity_found,
                                    image_file=image_file_item["title"],
                                    is_alternate_artwork=is_alternate_art
                                )
                                yugioh_set_card_image_file_list.append(
                                    list_obj.copy())
                                yugioh_set_cards.append(yugioh_set_card)

            while "continue" in res_json:
                card_list_obj_params["imcontinue"] = res_json["continue"]["imcontinue"]
                res_json = run_wiki_request_until_response(
                    MEDIAWIKI_URL, HEADERS, card_list_obj_params)
                if res_json:
                    res_json_query_obj: dict[str,
                                             dict] = res_json["query"]["pages"]
                    for set_card_gallery_page in res_json_query_obj.values():
                        yugioh_set_found: YugiohSet | None = next(
                            (ygo_set for ygo_set in yugioh_sets if ygo_set.yugipedia_set_card_gallery == set_card_gallery_page['title']), None)
                        if isinstance(yugioh_set_found, YugiohSet):
                            if "images" in set_card_gallery_page.keys():
                                for image_file_item in set_card_gallery_page["images"]:
                                    (check_is_yugioh_set_card, is_official_proxy) = is_image_file_yugioh_set_card(
                                        image_file_item["title"])
                                    if check_is_yugioh_set_card and not is_official_proxy:
                                        yugioh_rarity_found = None
                                        yugioh_card_found = None
                                        yugioh_card_image_name, yugioh_set_code, yugioh_set_language, yugioh_rarity_code, alternate_art_code, file_extension, is_alternate_art = get_split_data_from_image_file_v2(
                                            image_file=image_file_item["title"])

                                        yugioh_card_found = next(
                                            (ygo_card for ygo_card in yugioh_cards if ygo_card.card_image_name == yugioh_card_image_name), yugioh_card_found)
                                        yugioh_rarity_found = next(
                                            (ygo_card for ygo_card in yugioh_rarities if ygo_card.prefix == yugioh_rarity_code), yugioh_rarity_found)

                                        list_obj = {}
                                        list_obj["image_file"] = image_file_item["title"]
                                        list_obj["yugioh_set"] = yugioh_set_found
                                        yugioh_set_card = YugiohSetCard(
                                            yugioh_set=yugioh_set_found,
                                            yugioh_card=yugioh_card_found,
                                            yugioh_rarity=yugioh_rarity_found,
                                            image_file=image_file_item["title"],
                                            is_alternate_artwork=is_alternate_art
                                        )
                                        yugioh_set_card_image_file_list.append(
                                            list_obj.copy())
                                        yugioh_set_cards.append(
                                            yugioh_set_card)

                else:
                    break

    except Exception as e:
        print(e.args)
        pass

    return yugioh_set_card_image_file_list, yugioh_set_cards


def get_yugioh_set_card_image_url_from_yugioh_set_card_image_file_v2(yugioh_set_cards: List[YugiohSetCard]) -> List[YugiohSetCard]:
    yugioh_set_cards_updated: List[YugiohSetCard] = []
    image_file_obj_list: list[dict] = []
    image_file_strings: list[str] = [
        ygo_set_card.image_file for ygo_set_card in yugioh_set_cards if ygo_set_card.image_file is not None]
    card_list_obj_params = get_page_images_from_image_file_mediawiki_params(
        image_file_strings)

    set_dict_v2 = {
        ygo_set_card.image_file: ygo_set_card for ygo_set_card in yugioh_set_cards if ygo_set_card.image_file is not None}

    try:
        res_json = run_wiki_request_until_response(
            MEDIAWIKI_URL, HEADERS, card_list_obj_params)
        if res_json:
            res_json_query_obj: dict[str, dict] = res_json["query"]["pages"]
            res_json_query_obj = {
                key: value for key, value in res_json_query_obj.items() if int(key) >= 0}
            for page_image_obj in res_json_query_obj.values():
                yugioh_set_card_found = set_dict_v2.get(
                    page_image_obj['title'])
                if yugioh_set_card_found is not None:
                    if "original" in page_image_obj.keys():
                        yugioh_set_card_found.image_url = page_image_obj['original']['source']
                    yugioh_set_cards_updated.append(yugioh_set_card_found)
            while "continue" in res_json:
                card_list_obj_params["picontinue"] = res_json["continue"]["picontinue"]
                res_json = run_wiki_request_until_response(
                    MEDIAWIKI_URL, HEADERS, card_list_obj_params)
                if res_json:
                    res_json_query_obj = res_json["query"]["pages"]
                    res_json_query_obj = {
                        key: value for key, value in res_json_query_obj.items() if int(key) >= 0}
                    for page_image_obj in res_json_query_obj.values():
                        yugioh_set_card_found = set_dict_v2.get(
                            page_image_obj['title'])
                        if yugioh_set_card_found is not None:
                            if "original" in page_image_obj.keys():
                                yugioh_set_card_found.image_url = page_image_obj['original']['source']
                            yugioh_set_cards_updated.append(
                                yugioh_set_card_found)
                else:
                    break

    except Exception as e:
        print(e.args)
        pass

    return yugioh_set_cards_updated


def get_yugioh_set_cards_v2() -> tuple[list[YugiohSetCard], list[dict]]:
    yugioh_set_cards_v2: List[YugiohSetCard] = []
    yugioh_set_cards_v2_step2: List[YugiohSetCard] = []
    yugioh_set_cards_v2_overall: List[YugiohSetCard] = []
    yugioh_set_objs_from_db = retrieve_data_from_db_to_df(
        TABLE_YUGIOH_SETS, db_name='yugioh_data').to_dict(orient='records')
    yugioh_rarity_objs_from_db = retrieve_data_from_db_to_df(
        TABLE_YUGIOH_RARITIES, db_name='yugioh_data').to_dict(orient='records')
    yugioh_card_objs_from_db = retrieve_data_from_db_to_df(
        TABLE_YUGIOH_CARDS, db_name='yugioh_data').to_dict(orient='records')

    yugioh_sets: list[YugiohSet] = [YugiohSet.get_yugioh_set_from_db_obj(
        yugioh_set_obj) for yugioh_set_obj in yugioh_set_objs_from_db]
    yugioh_rarities: list[YugiohRarity] = [YugiohRarity.get_yugioh_rarity_from_db_obj(
        yugioh_rarity_obj) for yugioh_rarity_obj in yugioh_rarity_objs_from_db]
    yugioh_cards: list[YugiohCard] = [YugiohCard.get_yugioh_card_from_db_obj(
        yugioh_card_obj) for yugioh_card_obj in yugioh_card_objs_from_db]

    # to remove after testing
    # yugioh_sets = [
    #     ygo_set for ygo_set in yugioh_sets if ygo_set.set_code in ["QCAC", "SD5", "ADDR", "AGOV", "BC"]]
    # yugioh_sets = [
    #     ygo_set for ygo_set in yugioh_sets if ygo_set.set_code in ["ADDR", "SOVR"]]

    yugioh_set_split_list = list(split(yugioh_sets, 1))

    yugioh_set_card_image_file_overall_list: list[dict[str, str | YugiohSet | None]] = [
    ]

    # 1. get image_url out
    with ThreadPoolExecutor() as executor:  # optimally defined number of threads
        futures = []
        for yugioh_set_sublist in yugioh_set_split_list:
            futures.append(executor.submit(
                get_yugioh_set_card_image_file_v2, yugioh_set_sublist, yugioh_sets, yugioh_cards, yugioh_rarities))

        for future in concurrent.futures.as_completed(futures):
            result1, result2 = future.result()
            yugioh_set_card_image_file_overall_list.extend(result1)
            yugioh_set_cards_v2.extend(result2)

        print("Total image_card_url_overall_list items:{overall_list_count}".format(
            overall_list_count=len(yugioh_set_card_image_file_overall_list)))

    yugioh_set_cards_v2_split_list = list(
        split(yugioh_set_cards_v2, 25))

    # 2. from image_file to get image_url
    with ThreadPoolExecutor() as executor:  # optimally defined number of threads
        futures = []
        for split_list in yugioh_set_cards_v2_split_list:
            futures.append(executor.submit(
                get_yugioh_set_card_image_url_from_yugioh_set_card_image_file_v2, split_list))

        for future in concurrent.futures.as_completed(futures):
            result1 = future.result()
            if result1 is not None:
                yugioh_set_cards_v2_step2.extend(
                    result1)

        print("Total yugioh_set_cards_v2_step2 items:{overall_list_count}".format(
            overall_list_count=len(yugioh_set_cards_v2_step2)))
    # 3. get yugioh_set_card_from_set_card_codes
    yugioh_set_cards_v2_from_set_card_lists: List[YugiohSetCard] = []

    with ThreadPoolExecutor() as executor:  # optimally defined number of threads
        futures = []
        for split_list in yugioh_set_split_list:
            futures.append(executor.submit(
                get_yugioh_set_cards_from_set_card_list_names, split_list, yugioh_sets, yugioh_cards, yugioh_rarities))

        for future in concurrent.futures.as_completed(futures):
            time.sleep(1.0)
            result = future.result()
            yugioh_set_cards_v2_from_set_card_lists.extend(result)

        print("Total yugioh_set_cards_from_set_card_lists items:{overall_list_count}".format(
            overall_list_count=len(yugioh_set_cards_v2_from_set_card_lists)))
    print("i am almost done")
    yugioh_set_card_image_file_and_image_url_with_missing_links_overall_list: list[dict] = [
    ]

    yugioh_set_cards_v2_overall = consolidate_yugioh_set_cards(
        yugioh_set_cards_with_images=yugioh_set_cards_v2,
        yugioh_set_cards_with_codes=yugioh_set_cards_v2_from_set_card_lists
    )

    return yugioh_set_cards_v2_overall, yugioh_set_card_image_file_and_image_url_with_missing_links_overall_list


def consolidate_yugioh_set_cards(yugioh_set_cards_with_images: List[YugiohSetCard],
                                 yugioh_set_cards_with_codes: List[YugiohSetCard]):
    yugioh_set_cards_final: List[YugiohSetCard] = []

    ygo_set_card_with_code_dict = {
        "{region}|{set_name}|{card_english_name}|{rarity_name}".format(region=ygo_set_card.set.region if ygo_set_card.set is not None else "",
                                                                       set_name=ygo_set_card.set.name if ygo_set_card.set is not None else "",
                                                                       card_english_name=ygo_set_card.card.english_name if ygo_set_card.card is not None else "",
                                                                       rarity_name=ygo_set_card.rarity.name if ygo_set_card.rarity is not None else ""): ygo_set_card for ygo_set_card in yugioh_set_cards_with_codes if not ygo_set_card.code in (None, "")
    }
    for ygo_set_card_image in yugioh_set_cards_with_images:
        ygo_set_card_with_code_found = ygo_set_card_with_code_dict.get("{region}|{set_name}|{card_english_name}|{rarity_name}".format(region=ygo_set_card_image.set.region if ygo_set_card_image.set is not None else "",
                                                                                                                                      set_name=ygo_set_card_image.set.name if ygo_set_card_image.set is not None else "",
                                                                                                                                      card_english_name=ygo_set_card_image.card.english_name if ygo_set_card_image.card is not None else "",
                                                                                                                                      rarity_name=ygo_set_card_image.rarity.name if ygo_set_card_image.rarity is not None else ""), None)
        if ygo_set_card_with_code_found is not None:
            ygo_set_card_image.code = ygo_set_card_with_code_found.code
        yugioh_set_cards_final.append(ygo_set_card_image)

    ygo_set_card_final_dict = {
        "{region}|{set_name}|{card_english_name}|{rarity_name}".format(region=ygo_set_card.set.region if ygo_set_card.set is not None else "",
                                                                       set_name=ygo_set_card.set.name if ygo_set_card.set is not None else "",
                                                                       card_english_name=ygo_set_card.card.english_name if ygo_set_card.card is not None else "",
                                                                       rarity_name=ygo_set_card.rarity.name if ygo_set_card.rarity is not None else ""): ygo_set_card for ygo_set_card in yugioh_set_cards_final
    }
    for ygo_set_card_with_code in yugioh_set_cards_with_codes:
        ygo_set_card_with_image_found = ygo_set_card_final_dict.get("{region}|{set_name}|{card_english_name}|{rarity_name}".format(region=ygo_set_card_with_code.set.region if ygo_set_card_with_code.set is not None else "",
                                                                                                                                   set_name=ygo_set_card_with_code.set.name if ygo_set_card_with_code.set is not None else "",
                                                                                                                                   card_english_name=ygo_set_card_with_code.card.english_name if ygo_set_card_with_code.card is not None else "",
                                                                                                                                   rarity_name=ygo_set_card_with_code.rarity.name if ygo_set_card_with_code.rarity is not None else ""), None)

        if ygo_set_card_with_image_found is None:
            yugioh_set_cards_final.append(ygo_set_card_with_code)
    return yugioh_set_cards_final


def yugipedia_main():
    # Get the current project path
    project_path = os.getcwd()

    # Define the output folder relative to the project path
    output_folder = os.path.join(project_path, "output")
    # Check if the folder exists, and create it if it doesn't
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    ################################
    yugioh_cards = get_yugioh_cards()

    db_data = [yugioh_card.get_dict()
               for yugioh_card in yugioh_cards]

    if db_data == []:
        print("db_data is None")
        pass
    else:
        df = pd.DataFrame(db_data)
        df.to_csv(os.path.join(output_folder, "yugioh_cards.csv"), index=False)
        upload_data(df, table_name=TABLE_YUGIOH_CARDS,
                    if_exist="replace", db_name='yugioh_data')

        tekkx_cards = [
            YugiohCard.get_yugipedia_dict_from_yugioh_card(card) for card in yugioh_cards]

        df2 = pd.DataFrame(tekkx_cards)
        df2.to_csv(os.path.join(output_folder, "tekkx_cards.csv"), index=False)

    #################################
    yugioh_rarities = get_yugioh_rarities_v2()
    db_data = [yugioh_rarity.get_dict() for yugioh_rarity in yugioh_rarities]
    df = pd.DataFrame(db_data)
    df.to_csv(os.path.join(output_folder, "yugioh_rarities.csv"), index=False)
    upload_data(df, table_name=TABLE_YUGIOH_RARITIES,
                if_exist="replace", db_name='yugioh_data')

    #################################

    yugioh_sets = get_yugioh_sets_v2()
    db_data = [yugioh_set.get_dict() for yugioh_set in yugioh_sets]
    df = pd.DataFrame(db_data)
    df.to_csv(os.path.join(output_folder, "yugioh_sets.csv"), index=False)

    upload_data(df, table_name=TABLE_YUGIOH_SETS,
                if_exist="replace", db_name='yugioh_data')
    #################################


if __name__ == "__main__":
    yugipedia_main()
