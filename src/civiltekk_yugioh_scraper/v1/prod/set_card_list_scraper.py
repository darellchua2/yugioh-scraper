import re
from typing import List
import pandas as pd

from ..config import MEDIAWIKI_URL, HEADERS
from ..models.yugipedia_models import YugiohCard, YugiohSetCard, YugiohSet, YugiohRarity
from ..utilities.misc_utilities import run_wiki_request_until_response


def get_wikitext(page_titles: list[str]) -> dict[str, str]:
    """
    Fetch raw wikitext content for multiple Yugipedia pages at once.
    Returns a dict: { page_title: wikitext }
    """
    url = MEDIAWIKI_URL
    titles_param = "|".join(page_titles)

    params = {
        "action": "query",
        "format": "json",
        "prop": "revisions",
        "titles": titles_param,
        "rvprop": "content"
    }

    res_json = run_wiki_request_until_response(url, HEADERS, params)

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
    Parse card list from a dictionary of {page_title: wikitext}.
    Returns a combined DataFrame of all entries.
    """
    all_records = []

    for page_title, wikitext in wikitext_map.items():
        # Extract region and default rarity
        region_match = re.search(r'region=([A-Z]+)', wikitext)
        rarity_match = re.search(r'rarities=([\w,]+)', wikitext)

        region = region_match.group(1) if region_match else None
        default_rarities = rarity_match.group(
            1).split(",") if rarity_match else []

        # Match card lines (any set code format like AGOV-AE001)
        card_lines = re.findall(r'\n([A-Z]+-[A-Z]+[0-9]+[^\n]*)', wikitext)

        for line in card_lines:
            parts = [p.strip() for p in line.split(';')]
            set_card_code = parts[0] if len(parts) > 0 else None
            card_name = parts[1] if len(parts) > 1 else None
            rarities = parts[2] if len(
                parts) > 2 and parts[2] else ",".join(default_rarities)

            rarity_codes = [r.strip() for r in rarities.split(
                ',')] if rarities else default_rarities

            for rarity in rarity_codes:
                all_records.append({
                    "set_card_code": set_card_code,
                    "card_name": card_name,
                    "rarity_code": rarity,
                    "language": region,
                    "source_page": page_title
                })

    return pd.DataFrame(all_records)


def parse_set_lists_from_wikitext_map(wikitext_map: dict[str, str],
                                      yugioh_cards: List[YugiohCard],
                                      yugioh_sets: List[YugiohSet],
                                      yugioh_rarities: List[YugiohRarity]
                                      ) -> list[YugiohSetCard]:
    """
    Parse card list from a dictionary of {page_title: wikitext}.
    Returns a combined DataFrame of all entries.
    """
    all_records: List[YugiohSetCard] = []

    for page_title, wikitext in wikitext_map.items():
        yugioh_set_found = next(
            (s for s in yugioh_sets if s.yugipedia_set_card_list == page_title), None)
        if yugioh_set_found is None:
            print(f"Set not found: {page_title}")
            continue
        # Extract region and default rarity
        region_match = re.search(r'region=([A-Z]+)', wikitext)
        rarity_match = re.search(r'rarities=([\w,]+)', wikitext)

        region = region_match.group(1) if region_match else None
        default_rarities = rarity_match.group(
            1).split(",") if rarity_match else []

        # Match card lines (any set code format like AGOV-AE001)
        card_lines = re.findall(r'\n([A-Z]+-[A-Z]+[0-9]+[^\n]*)', wikitext)

        for line in card_lines:
            parts = [p.strip() for p in line.split(';')]
            set_card_code = parts[0] if len(parts) > 0 else None
            card_name = parts[1] if len(parts) > 1 else None
            yugioh_card_found = next(
                c for c in yugioh_cards if c.name == card_name)

            rarities = parts[2] if len(
                parts) > 2 and parts[2] else ",".join(default_rarities)

            rarity_codes = [r.strip() for r in rarities.split(
                ',')] if rarities else default_rarities
            for rarity_code in rarity_codes:
                yugioh_rarity_found = next(
                    (r for r in yugioh_rarities if r.prefix == rarity_code), None)
                if yugioh_rarity_found is None:
                    print(f"Rarity not found: {rarity_code}")
                    continue
                all_records.append(YugiohSetCard(
                    yugioh_card=yugioh_card_found,
                    yugioh_set=yugioh_set_found,
                    yugioh_rarity=yugioh_rarity_found,
                    code=set_card_code,
                ))
    return all_records


def scrape_main():
    # === USAGE ===
    page_titles = [
        "Set Card Lists:Age of Overlord (OCG-AE)", "Set Card Lists:Infinite Forbidden (OCG-AE)"]
    wikitext = get_wikitext(page_titles)
    df = parse_set_list(wikitext)
    df.to_csv("./output/test_scrape.csv", index=False)
    # Display or save result
    print(df.head())
    # df.to_csv("age_of_overlord_ocg_ae.csv", index=False)
