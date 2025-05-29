import re
from typing import List
import pandas as pd
import unicodedata


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
        rarity_match = re.search(r'rarities=([^\|\n]+)', wikitext)

        region = region_match.group(1) if region_match else None
        default_rarities = rarity_match.group(
            1).split(",") if rarity_match else []

        # Match card lines (any set code format like AGOV-AE001)
        card_lines = re.findall(r'\n([A-Z]+-[A-Z]+[0-9]+[^\n]*)', wikitext)

        for line in card_lines:
            clean_line = re.sub(r'//.*', '', line)  # remove inline comments
            parts = [p.strip() for p in clean_line.split(';')]
            set_card_code = parts[0] if len(parts) > 0 else None
            card_name = parts[1] if len(parts) > 1 else None
            rarities = parts[2] if len(
                parts) > 2 and parts[2] else ",".join(default_rarities)

            rarity_codes = [r.strip(" '\"") for r in rarities.split(
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


def find_card_by_name(name: str, cards: List[YugiohCard]) -> YugiohCard | None:
    name = normalize_card_name(name)
    card_lookup = {normalize_card_name(c.name): c for c in cards}
    return card_lookup.get(name, None)


def find_rarity(code_or_name: str, rarities: List[YugiohRarity]) -> YugiohRarity | None:
    """Find rarity by prefix or full name."""
    return next((r for r in rarities if r.prefix == code_or_name or r.name == code_or_name), None)


def parse_set_lists_from_wikitext_map(wikitext_map: dict[str, str],
                                      yugioh_cards: List[YugiohCard],
                                      yugioh_sets: List[YugiohSet],
                                      yugioh_rarities: List[YugiohRarity]
                                      ) -> list[YugiohSetCard]:
    """
    Parse card list from a dictionary of {page_title: wikitext}.
    Returns a list of YugiohSetCard objects.
    """
    all_records: List[YugiohSetCard] = []

    for page_title, wikitext in wikitext_map.items():
        yugioh_set = next(
            (s for s in yugioh_sets if s.yugipedia_set_card_list == page_title), None)
        if yugioh_set is None:
            print(f"⚠️ Set not found: {page_title}")
            continue

        # Parse region and default rarities
        region = re.search(r'region=([A-Z]+)', wikitext)
        region_code = region.group(1) if region else None
        default_rarities = parse_default_rarities(wikitext)

        card_lines = re.findall(r'\n([A-Z0-9]+-[A-Z]+[0-9]+[^\n]*)', wikitext)

        for line in card_lines:
            clean_line = clean_wiki_line(line)
            parts = [p.strip() for p in clean_line.split(';')]

            set_card_code = parts[0] if len(parts) > 0 else None
            raw_card_name = parts[1] if len(parts) > 1 else None
            card_name = raw_card_name

            if not card_name:
                print(f"⚠️ Missing card name in line: {line}")
                continue

            yugioh_card = find_card_by_name(card_name, yugioh_cards)
            if not yugioh_card:
                print(f"⚠️ Card not found: {card_name}")
                continue

            rarities_str = parts[2] if len(
                parts) > 2 else ",".join(default_rarities)
            rarity_codes = [r.strip(" '\"") for r in rarities_str.split(',')]

            for rarity_code in rarity_codes:
                rarity = find_rarity(rarity_code, yugioh_rarities)
                if rarity is None:
                    print(f"⚠️ Rarity not found: {rarity_code}")
                    continue

                all_records.append(YugiohSetCard(
                    yugioh_card=yugioh_card,
                    yugioh_set=yugioh_set,
                    yugioh_rarity=rarity,
                    code=set_card_code,
                ))

    return all_records


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
