import os
import time

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
from typing import Any, Dict, List, Optional, Sequence, TypedDict
from ..config import MEDIAWIKI_URL, HEADERS, TABLE_YUGIOH_CARDS, TABLE_YUGIOH_SETS, TABLE_YUGIOH_RARITIES
#### HEADERS START ####


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
        print(f"Fetching data with offset {offset}...")
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


def is_link_card_set_code(set_card_code) -> tuple[str | None, str | None]:
    pat1 = re.compile(
        r"^(.[^\-]{1,5})-(.[^\-]{1,5})$")
    pat_match = pat1.match(set_card_code)
    if pat_match:
        set_code = pat_match.group(1)
        return set_card_code, set_code
    else:
        return None, None


def get_split_data_from_image_file_v2(image_file: str) -> tuple[str | None, str | None, str | None, str | None, str | None, str | None, bool | None]:
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
        return None, None, None, None, None, None, None


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


def get_card_names_from_card_set_codes_redirect_mediawiki_params(card_set_codes: list[str]) -> dict[str, str | int]:
    card_set_codes_string = "|".join(card_set_codes)
    obj = {
        "action": "query",
        "format": "json",
        "prop": "pageprops",
        "titles": card_set_codes_string,
        "redirects": 1,
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


def get_yugioh_set_card_image_file(obj: dict) -> list[dict[str, str | YugiohSet]]:
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
    # in HC01, it was found that u need images as some images isnt uploaded.
    # so you cant use linkshere.
    # Linkshere only takes in images that have been uploaded
    yugioh_sets: list[YugiohSet] = obj["yugioh_sets"]
    # yugioh_cards: list[YugiohCard] = obj["yugioh_cards"]
    # yugioh_rarities: list[YugiohRarity] = obj["yugioh_rarities"]
    split_list = obj["split_list"]

    yugioh_set_card_image_file_list: list[dict] = []
    card_list_obj_params = get_set_card_gallery_mediawiki_params(
        [ygo_set.yugipedia_set_card_gallery for ygo_set in split_list if isinstance(ygo_set, YugiohSet)])
    set_dict = {
        yugioh_set.yugipedia_set_card_gallery: yugioh_set for yugioh_set in yugioh_sets}
    try:
        res_json = run_wiki_request_until_response(
            MEDIAWIKI_URL, HEADERS, card_list_obj_params)
        if res_json:
            res_json_query_obj: dict[str, dict] = res_json["query"]["pages"]
            for set_card_gallery_page in res_json_query_obj.values():
                # yugioh_set_found: YugiohSet | None = next(
                #     (ygo_set for ygo_set in yugioh_sets if ygo_set.yugipedia_set_card_gallery == set_card_gallery_page['title']), None)
                yugioh_set_found: YugiohSet | None = set_dict.get(
                    set_card_gallery_page['title'], None)
                if isinstance(yugioh_set_found, YugiohSet):
                    if "images" in set_card_gallery_page.keys():
                        for image_file_item in set_card_gallery_page["images"]:
                            (check_is_yugioh_set_card, is_official_proxy) = is_image_file_yugioh_set_card(
                                image_file_item["title"])
                            if check_is_yugioh_set_card and not is_official_proxy:
                                list_obj = {}
                                list_obj["image_file"] = image_file_item["title"]
                                # list_obj["set_name"] = yugioh_set_found.name
                                list_obj["yugioh_set"] = yugioh_set_found
                                # list_obj['region'] = yugioh_set_found.region
                                # list_obj['language'] = yugioh_set_found.language
                                yugioh_set_card_image_file_list.append(
                                    list_obj.copy())

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
                                        list_obj = {}
                                        list_obj["image_file"] = image_file_item["title"]
                                        # list_obj["set_name"] = yugioh_set_found.name
                                        list_obj["yugioh_set"] = yugioh_set_found
                                        # list_obj['region'] = yugioh_set_found.region
                                        list_obj['language'] = yugioh_set_found.language
                                        yugioh_set_card_image_file_list.append(
                                            list_obj.copy())
                else:
                    break

    except Exception as e:
        print(e.args)
        pass

    return yugioh_set_card_image_file_list


def get_yugioh_set_card_code_from_set_list(obj) -> tuple[list[dict[str, str | YugiohSet | None]], list[YugiohSet]]:
    yugioh_sets: list[YugiohSet] = obj["yugioh_sets"]
    # yugioh_cards: list[YugiohCard] = obj["yugioh_cards"]
    yugioh_rarities: list[YugiohRarity] = obj["yugioh_rarities"]
    split_list = obj["split_list"]

    yugioh_set_card_code_list: list[dict] = []
    yugioh_set_with_missing_links: list[YugiohSet] = []
    count = 0
    card_list_obj_params = get_set_card_list_links_mediawiki_params(
        [ygo_set.yugipedia_set_card_list for ygo_set in split_list if isinstance(ygo_set, YugiohSet)])
    try:
        res_json = run_wiki_request_until_response(
            MEDIAWIKI_URL, HEADERS, card_list_obj_params)
        count += 1
        if res_json:
            res_json_query_obj: dict = res_json["query"]["pages"]
            for set_card_list_page in res_json_query_obj.values():
                yugioh_set_found = next(
                    (ygo_set for ygo_set in yugioh_sets if ygo_set.yugipedia_set_card_list == set_card_list_page['title']), None)

                if "links" in set_card_list_page.keys():
                    for page_link_obj in set_card_list_page["links"]:
                        list_obj = {}
                        set_card_code_found, set_code_found = is_link_card_set_code(
                            page_link_obj["title"])
                        if set_card_code_found is not None and yugioh_set_found is not None:
                            list_obj["set_card_code"] = page_link_obj["title"]
                            # list_obj["set_name"] = yugioh_set_found.name
                            list_obj["yugioh_set"] = yugioh_set_found
                            list_obj['set_code'] = set_code_found
                            # list_obj['region'] = yugioh_set_found.region
                            if page_link_obj['title'] in ['SSB1-JPS01', 'SSB1-JPS02', 'SSB1-JPS03', 'SSB1-JPS04', 'SSB1-JPS05', 'SSB1-JPS06']:
                                list_obj['yugioh_rarity'] = next(
                                    (ygo_rarity for ygo_rarity in yugioh_rarities if ygo_rarity.prefix == "ScR"), None)
                                list_obj['rarity_name'] = "ScR"
                            if page_link_obj['title'] in ['SSB1-JP001', 'SSB1-JP002', 'SSB1-JP003', 'SSB1-JP004', 'SSB1-JP005', 'SSB1-JP006']:
                                list_obj['yugioh_rarity'] = next(
                                    (ygo_rarity for ygo_rarity in yugioh_rarities if ygo_rarity.prefix == "SR"), None)
                                list_obj['rarity_name'] = "SR"
                            yugioh_set_card_code_list.append(
                                list_obj.copy())

            while "continue" in res_json:
                card_list_obj_params["plcontinue"] = res_json["continue"]["plcontinue"]
                res_json = run_wiki_request_until_response(
                    MEDIAWIKI_URL, HEADERS, card_list_obj_params)
                if res_json:
                    res_json_query_obj: dict = res_json["query"]["pages"]
                    for set_card_list_page in res_json_query_obj.values():
                        yugioh_set_found = next(
                            (ygo_set for ygo_set in yugioh_sets if ygo_set.yugipedia_set_card_list == set_card_list_page['title']), None)
                        if "links" in set_card_list_page.keys():
                            for page_link_obj in set_card_list_page["links"]:
                                list_obj = {}
                                set_card_code_found, set_code_found = is_link_card_set_code(
                                    page_link_obj["title"])
                                if set_card_code_found is not None and yugioh_set_found is not None:
                                    list_obj["set_card_code"] = page_link_obj["title"]
                                    # list_obj["set_name"] = yugioh_set_found.name
                                    list_obj["yugioh_set"] = yugioh_set_found
                                    list_obj['set_code'] = set_code_found
                                    # list_obj['region'] = yugioh_set_found.region
                                    if page_link_obj['title'] in ['SSB1-JPS01', 'SSB1-JPS02', 'SSB1-JPS03', 'SSB1-JPS04', 'SSB1-JPS05', 'SSB1-JPS06']:
                                        list_obj['yugioh_rarity'] = next(
                                            (ygo_rarity for ygo_rarity in yugioh_rarities if ygo_rarity.prefix == "ScR"), None)
                                        list_obj['rarity_name'] = "ScR"
                                    if page_link_obj['title'] in ['SSB1-JP001', 'SSB1-JP002', 'SSB1-JP003', 'SSB1-JP004', 'SSB1-JP005', 'SSB1-JP006']:
                                        list_obj['yugioh_rarity'] = next(
                                            (ygo_rarity for ygo_rarity in yugioh_rarities if ygo_rarity.prefix == "SR"), None)
                                        list_obj['rarity_name'] = "SR"
                                    yugioh_set_card_code_list.append(
                                        list_obj.copy())
                else:
                    break

    except Exception as e:
        print(e.args)
        pass

    return yugioh_set_card_code_list, yugioh_set_with_missing_links


def get_yugioh_set_card_name_from_set_card_code(obj):
    yugioh_cards: list[YugiohCard] = obj["yugioh_cards"]
    split_list = obj["split_list"]

    yugioh_set_card_name_list: list[dict] = []
    params = get_card_names_from_card_set_codes_redirect_mediawiki_params(
        [split_list_item['set_card_code'] for split_list_item in split_list])
    set_card_code_dict = {
        split_list_item['set_card_code']: split_list_item for split_list_item in split_list}
    print("split_list")
    try:
        res_json = run_wiki_request_until_response(
            MEDIAWIKI_URL, HEADERS, params)
        if res_json:
            res_json_query_redirects_list = []
            if "redirects" in res_json["query"].keys():
                res_json_query_redirects_list = res_json["query"]["redirects"]

            for redirect_obj in res_json_query_redirects_list:
                split_list_item_found = set_card_code_dict.get(
                    redirect_obj['from'], None)
                yugioh_card_found: YugiohCard | None = next(
                    (ygo_card for ygo_card in yugioh_cards if ygo_card.name == redirect_obj['to']), None)
                if yugioh_card_found is not None and isinstance(split_list_item_found, dict):
                    split_list_item_found["yugioh_card"] = yugioh_card_found

                    yugioh_set_card_name_list.append(
                        split_list_item_found.copy())
    except Exception as e:
        print(e.args)
        pass

    return yugioh_set_card_name_list


def get_yugioh_set_card_image_url_from_yugioh_set_card_image_file(obj) -> list[dict]:
    # yugioh_sets: list[YugiohSet] = obj["yugioh_sets"]
    # yugioh_cards: list[YugiohCard] = obj["yugioh_cards"]
    # yugioh_rarities: list[YugiohRarity] = obj["yugioh_rarities"]
    split_list: list[dict[str, str | YugiohSet]] = obj["split_list"]

    image_file_obj_list: list[dict] = []
    image_file_strings: list[str] = [str(image_file_obj.get('image_file', ""))
                                     for image_file_obj in split_list]

    card_list_obj_params = get_page_images_from_image_file_mediawiki_params(
        image_file_strings)

    set_dict = {
        obj.get("image_file", ""): obj.get('yugioh_set') for obj in split_list if obj.get('yugioh_set') is not None}

    try:
        res_json = run_wiki_request_until_response(
            MEDIAWIKI_URL, HEADERS, card_list_obj_params)
        if res_json:
            res_json_query_obj: dict[str, dict] = res_json["query"]["pages"]
            res_json_query_obj = {
                key: value for key, value in res_json_query_obj.items() if int(key) >= 0}
            for page_image_obj in res_json_query_obj.values():
                image_file_found: dict | None = next(
                    (image_file_obj for image_file_obj in split_list if image_file_obj['image_file'] == page_image_obj['title']), None)
                if image_file_found is not None:
                    image_file_found['image_name'] = page_image_obj['pageimage']
                    if "original" in page_image_obj.keys():
                        image_file_found['image_url'] = page_image_obj['original']['source']
                    image_file_obj_list.append(image_file_found.copy())
            while "continue" in res_json:
                card_list_obj_params["picontinue"] = res_json["continue"]["picontinue"]
                res_json = run_wiki_request_until_response(
                    MEDIAWIKI_URL, HEADERS, card_list_obj_params)
                if res_json:
                    res_json_query_obj = res_json["query"]["pages"]
                    res_json_query_obj = {
                        key: value for key, value in res_json_query_obj.items() if int(key) >= 0}
                    for page_image_obj in res_json_query_obj.values():
                        image_file_found = next(
                            (image_file_obj for image_file_obj in split_list if image_file_obj['image_file'] == page_image_obj['title']), None)
                        if image_file_found is not None:
                            image_file_found['image_name'] = page_image_obj['pageimage']
                            if "original" in page_image_obj.keys():
                                image_file_found['image_url'] = page_image_obj['original']['source']
                            image_file_obj_list.append(image_file_found.copy())
                else:
                    break

    except Exception as e:
        print(e.args)
        pass

    return image_file_obj_list


def get_yugioh_set_card_relationship_if_available_from_yugioh_set_card_image_file(obj) -> tuple[list[dict], list[dict]]:
    yugioh_sets: list[YugiohSet] = obj["yugioh_sets"]
    yugioh_cards: list[YugiohCard] = obj["yugioh_cards"]
    yugioh_rarities: list[YugiohRarity] = obj["yugioh_rarities"]
    split_list = obj["split_list"]

    image_file_obj_list: list[dict] = []
    missing_links_image_file_obj_list: list[dict] = []

    card_list_obj_params = get_image_links_from_image_file_mediawiki_params(
        [image_card_url_obj['image_file'] for image_card_url_obj in split_list])
    try:
        res_json = run_wiki_request_until_response(
            MEDIAWIKI_URL, HEADERS, card_list_obj_params)
        if res_json:
            res_json_query_obj: dict[str, dict] = res_json["query"]["pages"]
            res_json_query_obj = {
                key: value for key, value in res_json_query_obj.items() if int(key) >= 0}
            for page_image_obj in res_json_query_obj.values():
                image_file_found: dict | None = next(
                    (image_file_obj for image_file_obj in split_list if image_file_obj['image_file'] == page_image_obj['title']), None)
                if image_file_found is not None:
                    if 'links' in page_image_obj.keys():
                        yugioh_card_found: YugiohCard | None = None
                        yugioh_rarity_found: YugiohRarity | None = None
                        # yugioh_set_found: YugiohSet = image_file_found['yugioh_set']
                        for link_obj in page_image_obj['links']:
                            link_obj_text: str = link_obj['title']
                            yugioh_card_found = next(
                                (ygo_card for ygo_card in yugioh_cards if ygo_card.name == link_obj_text), yugioh_card_found)
                            yugioh_rarity_found = next(
                                (ygo_rarity for ygo_rarity in yugioh_rarities if ygo_rarity.name == link_obj_text), yugioh_rarity_found)
                        # image_file_found['yugioh_set'] = yugioh_set_found
                        image_file_found['yugioh_rarity'] = yugioh_rarity_found
                        image_file_found['yugioh_card'] = yugioh_card_found
                    else:
                        image_file_found['yugioh_rarity'] = None
                        image_file_found['yugioh_card'] = None
                        missing_links_image_file_obj_list.append(
                            image_file_found.copy())

                    image_file_obj_list.append(image_file_found.copy())

            while "continue" in res_json:
                card_list_obj_params["picontinue"] = res_json["continue"]["picontinue"]
                res_json = run_wiki_request_until_response(
                    MEDIAWIKI_URL, HEADERS, card_list_obj_params)
                if res_json:
                    res_json_query_obj: dict[str,
                                             dict] = res_json["query"]["pages"]
                    res_json_query_obj = {
                        key: value for key, value in res_json_query_obj.items() if int(key) >= 0}
                    for page_image_obj in res_json_query_obj.values():
                        image_file_found: dict | None = next(
                            (image_file_obj for image_file_obj in split_list if image_file_obj['image_file'] == page_image_obj['title']), None)
                        # if image_file_found['image_file'] == "File:VoidFeast-TW01-JP-ScR.png":
                        #     print("i am here")
                        if image_file_found is not None:
                            if 'links' in page_image_obj.keys():
                                yugioh_card_found = None
                                yugioh_rarity_found = None
                                # yugioh_set_found: YugiohSet = image_file_found['yugioh_set']
                                for link_obj in page_image_obj['links']:
                                    link_obj_text: str = link_obj['title']
                                    yugioh_card_found = next(
                                        (ygo_card for ygo_card in yugioh_cards if ygo_card.name == link_obj_text), yugioh_card_found)
                                    yugioh_rarity_found = next(
                                        (ygo_rarity for ygo_rarity in yugioh_rarities if ygo_rarity.name == link_obj_text), yugioh_rarity_found)
                                    # yugioh_set_found = next(
                                    #     (ygo_set for ygo_set in yugioh_sets if ygo_set.name == link_obj_text), yugioh_set_found)

                                # image_file_found['yugioh_set'] = yugioh_set_found
                                image_file_found['yugioh_rarity'] = yugioh_rarity_found
                                image_file_found['yugioh_card'] = yugioh_card_found
                            else:
                                image_file_found['yugioh_rarity'] = None
                                image_file_found['yugioh_card'] = None
                                missing_links_image_file_obj_list.append(
                                    image_file_found.copy())

                            image_file_obj_list.append(image_file_found.copy())
                else:
                    break

    except Exception as e:
        print(e.args)
        pass

    return image_file_obj_list, missing_links_image_file_obj_list


def get_yugioh_set_cards() -> tuple[list[YugiohSetCard], list[dict]]:
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

    overall_obj = {}
    overall_obj["yugioh_sets"] = yugioh_sets
    overall_obj["yugioh_rarities"] = yugioh_rarities
    overall_obj["yugioh_cards"] = yugioh_cards

    # to remove after testing
    yugioh_sets = [
        ygo_set for ygo_set in yugioh_sets if ygo_set.set_code in ["TW01", "AGOV"]]

    yugioh_set_split_list = list(split(yugioh_sets, 1))

    yugioh_set_cards: list[YugiohSetCard] = []
    yugioh_set_card_image_file_overall_list: list[dict[str, str | YugiohSet | None]] = [
    ]
    yugioh_set_card_image_file_overall_list_with_image_urls: list[dict] = []
    yugioh_set_card_code_overall_list: list[dict[str, str | YugiohSet]] = []
    yugioh_set_with_missing_links_overall_list: list[YugiohSet] = []
    yugioh_set_card_code_overall_list_with_card_names: list[dict] = []

    # 1. get image_url out
    with ThreadPoolExecutor() as executor:  # optimally defined number of threads
        futures = []
        for split_list in yugioh_set_split_list:
            overall_obj["split_list"] = split_list
            futures.append(executor.submit(
                get_yugioh_set_card_image_file, overall_obj.copy()))

        for future in concurrent.futures.as_completed(futures):
            yugioh_set_card_image_file_overall_list.extend(future.result())

        print("Total image_card_url_overall_list items:{overall_list_count}".format(
            overall_list_count=len(yugioh_set_card_image_file_overall_list)))

    yugioh_set_card_image_file_overall_split_list: list[list[dict[str, str | YugiohSet | None]]] = list(
        split(yugioh_set_card_image_file_overall_list, 25))

    # 2. from image_file to get image_url
    with ThreadPoolExecutor() as executor:  # optimally defined number of threads
        futures = []
        for split_list in yugioh_set_card_image_file_overall_split_list:
            overall_obj["split_list"] = split_list
            futures.append(executor.submit(
                get_yugioh_set_card_image_url_from_yugioh_set_card_image_file, overall_obj.copy()))

        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            if result is not None:
                yugioh_set_card_image_file_overall_list_with_image_urls.extend(
                    result)

        print("Total yugioh_set_card_image_file_overall_list_with_image_urls items:{overall_list_count}".format(
            overall_list_count=len(yugioh_set_card_image_file_overall_list_with_image_urls)))

    yugioh_set_card_image_file_and_image_url_with_missing_links_overall_list: list[dict] = [
    ]

    # reset  yugioh_set_card_image_file_overall_list_with_image_urls
    yugioh_set_card_relationship_overall_list: list[dict] = []

    yugioh_set_card_image_file_and_image_url_overall_split_list: list[list[dict]] = list(
        split(yugioh_set_card_image_file_overall_list, 3))

    # reset  yugioh_set_card_image_file_overall_list_with_image_urls
    yugioh_set_card_relationship_overall_list: list[dict] = []

    # 3. from image_file to get image_url
    with ThreadPoolExecutor() as executor:  # optimally defined number of threads
        futures = []
        for split_list in yugioh_set_card_image_file_and_image_url_overall_split_list:
            overall_obj["split_list"] = split_list
            futures.append(executor.submit(
                get_yugioh_set_card_relationship_if_available_from_yugioh_set_card_image_file, overall_obj.copy()))

        for future in concurrent.futures.as_completed(futures):
            image_file_obj_list, missing_links_image_file_obj_list = future.result()
            yugioh_set_card_relationship_overall_list.extend(
                image_file_obj_list)
            yugioh_set_card_image_file_and_image_url_with_missing_links_overall_list.extend(
                missing_links_image_file_obj_list)

        print("Total yugioh_set_card_image_file_overall_list_with_image_urls items:{overall_list_count}".format(
            overall_list_count=len(yugioh_set_card_relationship_overall_list)))

    # 3. from image_file to get image_url
    # yugioh_set_card_image_file_overall_list_updated_2: list[dict[str, str | YugiohSet | None]] = [
    # ]

    # updated from yugioh_set_card_image_file_overall_list_updated to yugioh_set_card_image_file_overall_list_with_image_urls
    # for obj in yugioh_set_card_image_file_overall_list_with_image_urls:
    #     obj_updated = obj.copy()
    #     obj_found = next(
    #         (image_file_obj for image_file_obj in yugioh_set_card_relationship_overall_list
    #          if image_file_obj['image_file'] == obj_updated['image_file'] and
    #          'image_url' in image_file_obj.keys()),
    #         None)

    #     obj_updated['yugioh_card'] = obj_found['yugioh_card'] if obj_found is not None else None
    #     obj_updated['yugioh_rarity'] = obj_found['yugioh_rarity'] if obj_found is not None else None

    #     yugioh_set_card_image_file_overall_list_updated_2.append(
    #         obj_updated.copy())

    # 4. get from card_set_gallery to set_card_code list
    yugioh_set_split_list = list(split(yugioh_sets, 1))
    with ThreadPoolExecutor() as executor:  # optimally defined number of threads
        futures = []
        for split_list in yugioh_set_split_list:
            overall_obj["split_list"] = split_list
            futures.append(executor.submit(
                get_yugioh_set_card_code_from_set_list, overall_obj.copy()))

        for future in concurrent.futures.as_completed(futures):
            yugioh_set_card_code_list, yugioh_set_with_missing_links = future.result()

            yugioh_set_card_code_overall_list.extend(yugioh_set_card_code_list)
            yugioh_set_with_missing_links_overall_list.extend(
                yugioh_set_with_missing_links)

        print("Total yugioh_set_card_code_overall_list items:{overall_list_count}".format(
            overall_list_count=len(yugioh_set_card_code_overall_list)))

    yugioh_set_card_code_overall_split_list = list(
        split(yugioh_set_card_code_overall_list, 10))

    # 5. get from set_card_code mapped to yugioh card list
    with ThreadPoolExecutor() as executor:  # optimally defined number of threads
        futures = []
        for split_list in yugioh_set_card_code_overall_split_list:
            overall_obj["split_list"] = split_list
            futures.append(executor.submit(
                get_yugioh_set_card_name_from_set_card_code, overall_obj.copy()))
        for future in concurrent.futures.as_completed(futures):
            yugioh_set_card_code_overall_list_with_card_names.extend(
                future.result())

        print("Total yugioh_set_card_code_overall_list_with_card_names items:{overall_list_count}".format(
            overall_list_count=len(yugioh_set_card_code_overall_list_with_card_names)))

    # 6. fill in missing card and rarity
    # find missing card and rarity for objects that to date did not fill in all objects
    # up to this stage all is correct. ###################
    yugioh_set_card_image_file_overall_list_updated_2_missing = [obj for obj in yugioh_set_card_image_file_overall_list
                                                                 if obj.get('yugioh_rarity', None) is None or obj.get('yugioh_card', None) is None]

    yugioh_set_card_image_file_overall_list_updated_2_missing_split_list = list(
        split(yugioh_set_card_image_file_overall_list_updated_2_missing, 500))

    yugioh_set_cards: list[YugiohSetCard] = []
    yugioh_set_card_dict_list_found_from_missing_cards_and_rarity: list[dict] = [
    ]
    with ThreadPoolExecutor() as executor:  # optimally defined number of threads
        futures = []
        for split_list in yugioh_set_card_image_file_overall_list_updated_2_missing_split_list:
            overall_obj["yugioh_set_card_image_file_overall_list_with_image_urls"] = split_list
            overall_obj['yugioh_set_card_code_overall_list_with_card_names'] = yugioh_set_card_code_overall_list_with_card_names
            futures.append(executor.submit(
                get_yugioh_set_cards_from_information_obj_for_missing_links, overall_obj.copy()))
        for future in concurrent.futures.as_completed(futures):
            yugioh_set_card_dict_list_found_from_missing_cards_and_rarity.extend(
                future.result())

        print("Total yugioh_set_card_dict_list_found_from_missing_cards_and_rarity items:{overall_list_count}".format(
            overall_list_count=len(yugioh_set_card_dict_list_found_from_missing_cards_and_rarity)))

    # 6.2 update yugioh_set_card_image_file_overall_list_updated_2_filtered with missing information
    yugioh_set_card_image_file_overall_list_updated_3: list[dict] = []
    for dict_obj in yugioh_set_card_image_file_overall_list:
        dict_obj_updated = dict_obj.copy()
        if dict_obj_updated.get('yugioh_card', None) is None or dict_obj_updated.get("yugioh_rarity", None) is None:
            updated_dict_obj_from_step_6 = next(
                (obj for obj in yugioh_set_card_dict_list_found_from_missing_cards_and_rarity if obj['image_file'] == dict_obj_updated['image_file']), None)
            if updated_dict_obj_from_step_6 is not None:
                dict_obj_updated['yugioh_card'] = updated_dict_obj_from_step_6['yugioh_card']
                dict_obj_updated['yugioh_rarity'] = updated_dict_obj_from_step_6['yugioh_rarity']
        yugioh_set_card_image_file_overall_list_updated_3.append(
            dict_obj_updated.copy())

    yugioh_set_card_image_file_overall_list_updated_3_filtered_split_list = list(
        split(yugioh_set_card_image_file_overall_list_updated_3, 1000))

    # 7. consolidate results and create yugioh_set_card
    with ThreadPoolExecutor() as executor:  # optimally defined number of threads
        futures = []
        for split_list in yugioh_set_card_image_file_overall_list_updated_3_filtered_split_list:
            overall_obj["yugioh_set_card_image_file_overall_list_with_image_urls"] = split_list
            overall_obj['yugioh_set_card_code_overall_list_with_card_names'] = yugioh_set_card_code_overall_list_with_card_names
            futures.append(executor.submit(
                get_yugioh_set_cards_from_consolidated_list, overall_obj.copy()))
        for future in concurrent.futures.as_completed(futures):
            results: list[YugiohSetCard] = future.result()
            yugioh_set_cards.extend(
                results)

        print("Total yugioh_set_cards items:{overall_list_count}".format(
            overall_list_count=len(yugioh_set_cards)))

    return yugioh_set_cards, yugioh_set_card_image_file_and_image_url_with_missing_links_overall_list


def get_yugioh_set_cards_from_information_obj_for_missing_links(overall_obj: dict) -> list[dict]:
    yugioh_sets: List[YugiohSet] = overall_obj['yugioh_sets']
    yugioh_cards: list[YugiohCard] = overall_obj["yugioh_cards"]
    yugioh_rarities: list[YugiohRarity] = overall_obj["yugioh_rarities"]
    yugioh_set_card_image_file_overall_list_with_image_urls: list[
        dict] = overall_obj['yugioh_set_card_image_file_overall_list_with_image_urls']
    yugioh_set_card_code_overall_list_with_card_names: list[dict] = overall_obj[
        "yugioh_set_card_code_overall_list_with_card_names"]

    yugioh_set_card_dict_list: list[dict] = []
    for image_url_obj in yugioh_set_card_image_file_overall_list_with_image_urls:
        image_url_obj_updated = image_url_obj.copy()
        yugioh_set_found: YugiohSet | None = image_url_obj_updated.get(
            'yugioh_set', None)
        yugioh_card_found: YugiohCard | None = image_url_obj_updated.get(
            'yugioh_card', None)
        yugioh_rarity_found: YugiohRarity | None = image_url_obj_updated.get(
            'yugioh_rarity', None)
        yugioh_set_found: YugiohSet | None = image_url_obj_updated['yugioh_set']
        yugioh_card_image_name, yugioh_set_code, yugioh_set_language, yugioh_rarity_code, alternate_art_code, file_extension, is_alternate_art = get_split_data_from_image_file_v2(
            image_file=image_url_obj_updated['image_file'])
        if yugioh_card_found is None:
            yugioh_card_found = next(
                (ygo_card for ygo_card in yugioh_cards if ygo_card.card_image_name == yugioh_card_image_name), yugioh_card_found)
        if yugioh_rarity_found is None:
            yugioh_rarity_found = next(
                (ygo_card for ygo_card in yugioh_rarities if ygo_card.prefix == yugioh_rarity_code), yugioh_rarity_found)
        # make sure i use the right yugioh_set
        yugioh_set_found = next(
            (ygo_set for ygo_set in yugioh_sets if ygo_set.set_code == yugioh_set_code and ygo_set.language == yugioh_set_language), yugioh_set_found)

        # there are cards with alternate passwords so the card_image_name will be the same
        # use this filter to find if it is either 1 of the two then only select the 1 that exists in the overall_set_card_code_list
        yugioh_cards_found = [
            ygo_card for ygo_card in yugioh_cards if ygo_card.card_image_name == yugioh_card_image_name]

        # CHECKING FOR BACH-JPT01 BECAUSE IT IS NOT AN ACTUAL CARD. SO have to filter by 'yugioh_card'
        yugioh_set_card_code_overall_list_with_card_names_filtered: list = [
            obj for obj in yugioh_set_card_code_overall_list_with_card_names if obj['set_code'] == yugioh_set_code]
        if len(yugioh_set_card_code_overall_list_with_card_names_filtered) > 0:
            objs_found: list[dict] = [
                obj for obj in yugioh_set_card_code_overall_list_with_card_names_filtered if 'yugioh_card' in obj and obj['yugioh_card'] in yugioh_cards_found]
            obj_found = {}
            if len(objs_found) > 1 and all("yugioh_rarity" in obj.keys() for obj in objs_found):
                obj_found = next(
                    (obj for obj in objs_found if 'yugioh_rarity' in obj.keys() and obj['yugioh_rarity'] == yugioh_rarity_found), {})
            else:
                obj_found: dict = next(
                    (obj for obj in yugioh_set_card_code_overall_list_with_card_names_filtered if 'yugioh_card' in obj and obj['yugioh_card'] in yugioh_cards_found), {})
            if isinstance(obj_found, dict) and "yugioh_card" in obj_found.keys():
                yugioh_card_found = obj_found['yugioh_card']
                if 'yugioh_rarity' in obj_found.keys():
                    yugioh_rarity_found = obj_found['yugioh_rarity']

        # image_url_obj_updated['yugioh_set'] = yugioh_set_found
        image_url_obj_updated['yugioh_card'] = yugioh_card_found
        image_url_obj_updated['yugioh_rarity'] = yugioh_rarity_found
        image_url_obj_updated['yugioh_set'] = yugioh_set_found
        yugioh_set_card_dict_list.append(image_url_obj_updated.copy())

    return yugioh_set_card_dict_list


def get_yugioh_set_cards_from_consolidated_list(overall_obj: dict) -> list[YugiohSetCard]:
    yugioh_sets: list[YugiohSet] = overall_obj["yugioh_sets"]
    yugioh_cards: list[YugiohCard] = overall_obj["yugioh_cards"]
    yugioh_rarities: list[YugiohRarity] = overall_obj["yugioh_rarities"]
    yugioh_set_card_image_file_overall_list_with_image_urls: list[
        dict] = overall_obj['yugioh_set_card_image_file_overall_list_with_image_urls']
    yugioh_set_card_code_overall_list_with_card_names: list[dict] = overall_obj[
        "yugioh_set_card_code_overall_list_with_card_names"]

    yugioh_set_cards: list[YugiohSetCard] = []
    yugioh_set_card_dict_list: list[dict] = []
    # replace from yugioh_set_card_image_file_overall_list_with_image_urls to yugioh_set_card_code_overall_list_with_card_names
    for dict_obj in yugioh_set_card_image_file_overall_list_with_image_urls:
        dict_obj_updated = dict_obj.copy()
        yugioh_set_card_code_found: str | None = None
        yugioh_set_found: YugiohSet | None = dict_obj_updated.get(
            'yugioh_set', None)
        yugioh_card_found: YugiohCard | None = dict_obj_updated.get(
            'yugioh_card', None)
        yugioh_rarity_found: YugiohRarity | None = dict_obj_updated.get(
            'yugioh_rarity', None)

        yugioh_card_image_name, yugioh_set_code, yugioh_set_language, yugioh_rarity_code, alternate_art_code, file_extension, is_alternate_art = get_split_data_from_image_file_v2(
            image_file=dict_obj_updated['image_file'])

        # there are cards with alternate passwords so the card_image_name will be the same
        # use this filter to find if it is either 1 of the two then only select the 1 that exists in the overall_set_card_code_list
        yugioh_cards_found = [
            ygo_card for ygo_card in yugioh_cards if ygo_card.card_image_name == yugioh_card_image_name]

        # CHECKING FOR BACH-JPT01 BECAUSE IT IS NOT AN ACTUAL CARD. SO have to filter by 'yugioh_card'
        yugioh_set_card_code_overall_list_with_card_names_filtered: list = [
            obj for obj in yugioh_set_card_code_overall_list_with_card_names if obj['set_code'] == yugioh_set_code]
        if len(yugioh_set_card_code_overall_list_with_card_names_filtered) > 0:
            objs_found = [
                obj for obj in yugioh_set_card_code_overall_list_with_card_names_filtered if 'yugioh_card' in obj and obj['yugioh_card'] in yugioh_cards_found]
            obj_found: dict = {}
            if len(objs_found) > 1 and all("yugioh_rarity" in obj.keys() for obj in objs_found):
                obj_found = next(
                    (obj for obj in objs_found if 'yugioh_rarity' in obj.keys() and obj['yugioh_rarity'] == yugioh_rarity_found), {})
            else:
                obj_found: dict = next(
                    (obj for obj in yugioh_set_card_code_overall_list_with_card_names_filtered if 'yugioh_card' in obj and obj['yugioh_card'] in yugioh_cards_found), {})
            if "yugioh_card" in obj_found.keys():
                yugioh_card_found = obj_found['yugioh_card']
                yugioh_set_card_code_found = obj_found['set_card_code']
                if 'yugioh_rarity' in obj_found.keys():
                    yugioh_rarity_found = obj_found['yugioh_rarity']

        yugioh_set_card_dict_list.append(dict_obj_updated)

        is_image_file_language_same_as_set_language = yugioh_set_found.language == yugioh_set_language if isinstance(
            yugioh_set_found, YugiohSet) else False

        if isinstance(yugioh_card_found, YugiohCard) and isinstance(yugioh_set_found, YugiohSet) and isinstance(yugioh_rarity_found, YugiohRarity) and isinstance(is_alternate_art, bool):
            yugioh_set_card = YugiohSetCard(
                yugioh_set=yugioh_set_found,
                yugioh_card=yugioh_card_found,
                yugioh_rarity=yugioh_rarity_found,
                code=yugioh_set_card_code_found,
                image_url=dict_obj_updated.get(
                    'image_url', None) if is_image_file_language_same_as_set_language else None,
                image_file=dict_obj_updated.get(
                    'image_file', None) if is_image_file_language_same_as_set_language else None,
                is_alternate_artwork=is_alternate_art,

            )
            yugioh_set_cards.append(yugioh_set_card)

    return yugioh_set_cards


def yugipedia_main():
    # Get the current project path
    project_path = os.getcwd()

    # Define the output folder relative to the project path
    output_folder = os.path.join(project_path, "output")
    # Check if the folder exists, and create it if it doesn't
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

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

    ################################


if __name__ == "__main__":
    yugipedia_main()
