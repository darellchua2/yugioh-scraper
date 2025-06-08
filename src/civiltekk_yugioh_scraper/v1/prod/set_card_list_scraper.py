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
    Parse card lists from a dictionary of {page_title: wikitext}.
    Returns a combined DataFrame of all entries.
    Supports global and entry-specific fields like qty, description, rarities.
    """
    all_records = []

    for page_title, wikitext in wikitext_map.items():
        # Find all Set list blocks
        set_blocks = re.findall(r"{{Set list\|(.+?)}}", wikitext, re.DOTALL)
        for block in set_blocks:
            lines = block.strip().splitlines()

            # Extract global parameters
            global_params = {}
            entry_lines = []
            for line in lines:
                if ';' in line:
                    entry_lines.append(line)
                else:
                    # Handle key=value pairs
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

            for line in entry_lines:
                if not line.strip():
                    continue

                # Remove inline comment
                parts_main = line.split('//', 1)
                entry_data = parts_main[0].strip()
                entry_opts = parts_main[1].strip() if len(
                    parts_main) > 1 else ""

                fields = [p.strip() for p in entry_data.split(';')]
                while len(fields) < 5:
                    fields.append("")

                set_card_code, name, rarity, print_code, quantity = fields[:5]
                rarities = rarity if rarity else ",".join(default_rarities)
                rarity_list = [r.strip() for r in rarities.split(
                    ',')] if rarities else default_rarities

                # Parse entry options
                entry_opts_dict = {}
                for opt in re.split(r',|\s', entry_opts):
                    if '=' in opt:
                        k, v = opt.split('=', 1)
                        entry_opts_dict[k.strip()] = v.strip()

                for r in rarity_list:
                    record = {
                        "set_card_code": set_card_code,
                        "card_name": name,
                        "rarity_code": r,
                        "language": region,
                        "print_code": print_code,
                        "source_page": page_title
                    }

                    if use_qty:
                        record["quantity"] = quantity
                    if use_description:
                        record["description"] = entry_opts_dict.get(
                            "description", "")

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
    Parse card lists from a dictionary of {page_title: wikitext}.
    Returns a list of YugiohSetCard objects with proper rarity, description, and quantity handling.
    """
    all_records: List[YugiohSetCard] = []

    for page_title, wikitext in wikitext_map.items():
        yugioh_set = next(
            (s for s in yugioh_sets if s.yugipedia_set_card_list == page_title), None)
        if yugioh_set is None:
            print(f"⚠️ Set not found: {page_title}")
            continue

        # Extract all Set list blocks
        set_blocks = re.findall(r"{{Set list\|(.+?)}}", wikitext, re.DOTALL)
        for block in set_blocks:
            lines = block.strip().splitlines()

            # Extract global parameters
            global_params = {}
            entry_lines = []
            for line in lines:
                if ';' in line:
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

            for line in entry_lines:
                if not line.strip():
                    continue

                parts_main = line.split('//', 1)
                entry_data = parts_main[0].strip()
                entry_opts = parts_main[1].strip() if len(
                    parts_main) > 1 else ""

                fields = [p.strip() for p in entry_data.split(';')]
                while len(fields) < 5:
                    fields.append("")

                set_card_code, raw_name, rarity, print_code, quantity = fields[:5]
                card_name = normalize_card_name(raw_name)
                # Detect alternate artwork
                is_alternate_artwork = any(
                    marker in card_name.lower()
                    for marker in ("(alternate artwork)", "(international artwork)", "(new artwork)")
                )

                # Optionally remove the marker from the display name
                card_name = re.sub(r'\s*\((alternate|international|9th|8th|new|7th) artwork\)',
                                   '', card_name, flags=re.IGNORECASE).strip()

                yugioh_card = find_card_by_name(card_name, yugioh_cards)
                if not yugioh_card:
                    print(f"⚠️ Card not found: {card_name}")
                    continue

                rarities = rarity if rarity else ",".join(default_rarities)
                rarity_list = [r.strip() for r in rarities.split(
                    ',')] if rarities else default_rarities

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
                    )

                    # Add optional fields
                    # if use_qty:
                    #     try:
                    #         set_card.quantity = int(quantity)
                    #     except ValueError:
                    #         set_card.quantity = None
                    # if use_description:
                    #     set_card.description = entry_opts_dict.get(
                    #         "description", "")

                    # 🔍 Set the alternate artwork flag
                    set_card.is_alternate_artwork = is_alternate_artwork

                    all_records.append(set_card)

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
