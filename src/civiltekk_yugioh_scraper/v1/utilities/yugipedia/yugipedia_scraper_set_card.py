from concurrent.futures import ThreadPoolExecutor
import concurrent.futures

import re
import time
from typing import List, Mapping
import pandas as pd
import unicodedata
import mwparserfromhell

from ..aws_utilities import retrieve_data_from_db_to_df


from ...config import MEDIAWIKI_URL, HEADERS
from ...models.yugipedia_models import YugiohCard, YugiohSetCard, YugiohSet, YugiohRarity
from ..misc_utilities import run_wiki_request_until_response, split
from .mediawiki_params import get_wikitext_params, get_set_card_gallery_mediawiki_params, get_page_images_from_image_file_mediawiki_params

from ...config import MEDIAWIKI_URL, HEADERS, TABLE_YUGIOH_CARDS, TABLE_YUGIOH_SETS, TABLE_YUGIOH_RARITIES


def get_wikitext(page_titles: list[str]) -> dict[str, str]:
    """
    Fetch raw wikitext content for multiple Yugipedia pages at once.
    Returns a dict: { page_title: wikitext }
    """
    url = MEDIAWIKI_URL
    titles_param = "|".join(page_titles)
    params: Mapping[str, str | int] = get_wikitext_params(
        titles_str=titles_param)

    res_json = run_wiki_request_until_response(
        url=url, header=HEADERS, params=params)

    wikitext_map = {}
    if res_json:
        pages = res_json["query"]["pages"]
        for page_data in pages.values():
            title = page_data.get("title")
            revision = page_data.get("revisions", [{}])[0]
            content = revision.get("*")  # Legacy content key
            if title and content:
                wikitext_map[title] = content

    return wikitext_map


def parse_set_list(wikitext_map: dict[str, str]) -> pd.DataFrame:
    """
    Parse card lists from a dictionary of {page_title: wikitext}.
    Returns a combined DataFrame of all entries.
    Supports `noabbr`, entry-specific options, quantity, and description.
    """
    all_records = []

    for page_title, wikitext in wikitext_map.items():
        set_blocks = re.findall(
            r"{{Set list\|([\s\S]*?)\n}}", wikitext, re.DOTALL)

        for block in set_blocks:
            lines = block.strip().splitlines()

            # Extract global params
            global_params = {}
            entry_lines = []
            for line in lines:
                if ';' in line or line.strip():
                    entry_lines.append(line)
                else:
                    parts = line.strip().split('|')
                    for p in parts:
                        if '=' in p:
                            k, v = p.split('=', 1)
                            global_params[k.strip()] = v.strip()

            region = global_params.get("region", None)
            default_rarities = [r.strip() for r in global_params.get(
                "rarities", "").split(',') if r.strip()]
            use_qty = global_params.get(
                "qty", "0").lower() in ("1", "true", "yes", "y")
            use_description = global_params.get(
                "description", "0").lower() in ("1", "true", "yes", "y")
            is_noabbr = "noabbr" in global_params.get("options", "").lower()

            for line in entry_lines:
                if not line.strip():
                    continue

                parts_main = line.split('//', 1)
                entry_data = parts_main[0].strip()
                entry_opts = parts_main[1].strip() if len(
                    parts_main) > 1 else ""

                fields = [p.strip() for p in entry_data.split(';')]

                # Determine field parsing based on noabbr or length
                if is_noabbr or len(fields) == 1:
                    set_card_code = None
                    raw_name = fields[0]
                    rarity = ",".join(default_rarities)
                    print_code = ""
                    quantity = ""
                else:
                    while len(fields) < 5:
                        fields.append("")
                    set_card_code, raw_name, rarity, print_code, quantity = fields[:5]

                card_name = normalize_card_name(raw_name)

                # Detect alternate artwork
                is_alternate_artwork = any(
                    marker in card_name.lower()
                    for marker in (
                        "(alternate artwork)",
                        "(international artwork)",
                        "(new artwork)",
                        "(9th artwork)",
                        "(8th artwork)",
                        "(7th artwork)",
                    )
                )
                card_name = re.sub(
                    r'\s*\((alternate|international|9th|8th|new|7th) artwork\)',
                    '',
                    card_name,
                    flags=re.IGNORECASE
                ).strip()

                rarities = rarity if rarity else ",".join(default_rarities)
                rarity_list = [r.strip() for r in rarities.split(
                    ',')] if rarities else default_rarities

                # Parse entry options (description, etc.)
                entry_opts_dict = {}
                for opt in re.split(r',|\s', entry_opts):
                    if '=' in opt:
                        k, v = opt.split('=', 1)
                        entry_opts_dict[k.strip()] = v.strip()

                for r in rarity_list:
                    record = {
                        "set_card_code": set_card_code,
                        "card_name": card_name,
                        "rarity_code": r,
                        "language": region,
                        "print_code": print_code,
                        "source_page": page_title,
                        "is_alternate_artwork": is_alternate_artwork,
                    }

                    # if use_qty:
                    #     record["quantity"] = quantity
                    # if use_description:
                    #     record["description"] = entry_opts_dict.get(
                    #         "description", "")

                    all_records.append(record)

    return pd.DataFrame(all_records)


def clean_wiki_line(line: str) -> str:
    """Remove inline comments such as //description::... or //notes"""
    return re.sub(r'//.*', '', line).strip()


def normalize_card_name(name: str) -> str:
    """Normalize card names by removing invisible chars and applying Unicode normalization."""
    name = re.sub(r'[\u200e\u200f\u202a-\u202e\u2060-\u206f]', '', name)
    name = unicodedata.normalize('NFKC', name)
    return name.strip()


def parse_default_rarities(wikitext: str) -> List[str]:
    """Extract and clean default rarities from the header."""
    match = re.search(r'rarities=([^\|\n]+)', wikitext)
    if match:
        return [r.strip(" '\"") for r in match.group(1).split(',')]
    return []


def find_card_by_english_name(name: str, cards: List[YugiohCard]) -> YugiohCard | None:
    name = normalize_card_name(name).replace(
        " (card)", "").replace(" (Arkana)", "")
    card_lookup = {normalize_card_name(c.english_name): c for c in cards}
    return card_lookup.get(name, None)


def find_rarity(code_or_name: str, rarities: List[YugiohRarity]) -> YugiohRarity | None:
    """Find rarity by prefix or full name."""
    return next((r for r in rarities if r.prefix == code_or_name or r.name == code_or_name), None)


def clean_wikitext_specials(text: str) -> str:
    """
    Replace MediaWiki special templates like {{=}} with literal characters.
    """
    return text.replace('{{=}}', '=').replace('{{!}}', '|').strip()


def parse_set_lists_from_wikitext_map(wikitext_map: dict[str, str],
                                      yugioh_cards: List[YugiohCard],
                                      yugioh_sets: List[YugiohSet],
                                      yugioh_rarities: List[YugiohRarity]
                                      ) -> list[YugiohSetCard]:
    """
    Parse card lists from a dictionary of {page_title: wikitext}.
    Returns a list of YugiohSetCard objects with proper rarity, description, and quantity handling.
    """
    all_records: List[YugiohSetCard] = []

    for page_title, wikitext in wikitext_map.items():
        print(f"Processing page: {page_title}")
        print(wikitext[:500])

        yugioh_set = next(
            (s for s in yugioh_sets if s.yugipedia_set_card_list == page_title), None)
        if yugioh_set is None:
            print(f"⚠️ Set not found: {page_title}")
            continue

        wikicode = mwparserfromhell.parse(wikitext)
        for template in wikicode.filter_templates():
            if not template.name.matches("Set list"):
                continue

            # Extract global parameters from the header
            header_line = str(template.params[0].value).strip(
            ) if template.params else ""
            global_params = {k.strip(): v.strip() for k, v in (
                param.split('=', 1) for param in header_line.split('|') if '=' in param)}
            default_rarities = [r.strip() for r in global_params.get(
                "rarities", "").split(',') if r.strip()]

            if not default_rarities:
                print(f"⚠️ No global rarity found in header for {page_title}")

            # Process the content (card lines) — skip the header line
            content_lines = str(template).split('\n')[1:]
            for line in content_lines:
                line = line.strip()
                if not line or line.startswith('|'):
                    continue

                parts_main = line.split('//', 1)
                entry_data = parts_main[0].strip()
                entry_opts = parts_main[1].strip() if len(
                    parts_main) > 1 else ""

                is_alternate_artwork = any(
                    marker in entry_opts.lower()
                    for marker in ("alternate artwork", "international artwork", "new artwork", "9th artwork", "8th artwork", "7th artwork")
                )

                fields = [p.strip() for p in entry_data.split(';')]
                if len(fields) < 5:
                    print(f"⚠️ Incomplete data for entry: {line}")
                    fields += [""] * (5 - len(fields))
                    print(f"Fixed fields: {fields}")

                set_card_code, raw_name, raw_rarity, print_code, quantity = fields[:5]
                raw_name = clean_wikitext_specials(raw_name)
                card_name = normalize_card_name(raw_name)
                print(f"Normalized card name: {card_name}")

                yugioh_card = find_card_by_english_name(
                    card_name, yugioh_cards)
                if not yugioh_card:
                    print(f"⚠️ Card not found: {card_name}")
                    continue

                rarities = raw_rarity.split(
                    ',') if raw_rarity else default_rarities
                rarity_list = [r.strip() for r in rarities if r.strip()]

                entry_opts_dict = {}
                for opt in re.split(r',|\s', entry_opts):
                    if '=' in opt:
                        k, v = opt.split('=', 1)
                        entry_opts_dict[k.strip()] = v.strip()

                for r_code in rarity_list:
                    rarity_obj = find_rarity(r_code, yugioh_rarities)
                    if rarity_obj is None:
                        print(f"⚠️ Rarity not found: {r_code}")
                        continue

                    set_card = YugiohSetCard(
                        yugioh_card=yugioh_card,
                        yugioh_set=yugioh_set,
                        yugioh_rarity=rarity_obj,
                        code=set_card_code,
                        is_alternate_artwork=is_alternate_artwork
                    )
                    all_records.append(set_card)

    return all_records


def is_image_file_yugioh_set_card(image_file) -> tuple[bool, bool]:
    pat1 = re.compile(
        r"^File:(.[^\-]+)-(.[^\-]+)-(.[^\-]+)-(.[^\-]*)-?(.[^\-]*)?(.png|.jpg|.jpeg|.gif)$")
    pat_match = pat1.match(image_file)
    if pat_match:
        is_offcial_proxy: bool = True if pat_match.group(4) == "OP" else False
        return True, is_offcial_proxy
    return False, False


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


def get_yugioh_set_cards_from_set_card_list_names(split_list: List[YugiohSet],
                                                  yugioh_sets: List[YugiohSet],
                                                  yugioh_cards: List[YugiohCard],
                                                  yugioh_rarities: List[YugiohRarity]) -> List[YugiohSetCard]:
    page_titles: list[str] = [
        ygo_set.yugipedia_set_card_list for ygo_set in split_list]
    wikitext_map = get_wikitext(page_titles=page_titles)
    yugioh_set_cards = parse_set_lists_from_wikitext_map(wikitext_map=wikitext_map,
                                                         yugioh_cards=yugioh_cards,
                                                         yugioh_sets=yugioh_sets,
                                                         yugioh_rarities=yugioh_rarities)
    return yugioh_set_cards


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


def consolidate_yugioh_set_cards_v2(yugioh_set_cards_with_images: List[YugiohSetCard],
                                    yugioh_set_cards_with_codes: List[YugiohSetCard]) -> List[YugiohSetCard]:

    def key(ygo_set_card: YugiohSetCard):
        return "{region}|{set_name}|{card_english_name}|{rarity_name}".format(
            region=ygo_set_card.set.region if ygo_set_card.set else "",
            set_name=ygo_set_card.set.name if ygo_set_card.set else "",
            card_english_name=ygo_set_card.card.english_name if ygo_set_card.card else "",
            rarity_name=ygo_set_card.rarity.name if ygo_set_card.rarity else ""
        )

    # Create dict from code list (only those with non-empty code)
    ygo_set_card_with_code_dict = {
        key(ygo): ygo for ygo in yugioh_set_cards_with_codes if ygo.code not in (None, "")
    }

    # Update images with code if found
    updated_images = [
        (
            setattr(ygo_image, 'code',
                    ygo_set_card_with_code_dict[key(ygo_image)].code)
            or ygo_image
        ) if key(ygo_image) in ygo_set_card_with_code_dict else ygo_image
        for ygo_image in yugioh_set_cards_with_images
    ]

    # Build final dict from updated images
    ygo_set_card_final_dict = {key(ygo): ygo for ygo in updated_images}

    # Add codes that are not already in final list
    final_list = updated_images + [
        ygo_code for ygo_code in yugioh_set_cards_with_codes
        if key(ygo_code) not in ygo_set_card_final_dict
    ]

    return final_list


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
    yugioh_sets = [
        ygo_set for ygo_set in yugioh_sets if ygo_set.set_code in ["ADDR", "SOVR", "DUAD"]]

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

    yugioh_set_cards_v2_overall = consolidate_yugioh_set_cards_v2(
        yugioh_set_cards_with_images=yugioh_set_cards_v2,
        yugioh_set_cards_with_codes=yugioh_set_cards_v2_from_set_card_lists
    )

    return yugioh_set_cards_v2_overall, yugioh_set_card_image_file_and_image_url_with_missing_links_overall_list


def scrape_main():
    # === USAGE ===
    page_titles = [
        "Set Card Lists:Age of Overlord (OCG-AE)", "Set Card Lists:Infinite Forbidden (OCG-AE)"]
    wikitext_map = get_wikitext(page_titles)
    df = parse_set_list(wikitext_map)
    df.to_csv("./output/test_scrape.csv", index=False)
    # Display or save result
    print(df.head())
    # df.to_csv("age_of_overlord_ocg_ae.csv", index=False)
