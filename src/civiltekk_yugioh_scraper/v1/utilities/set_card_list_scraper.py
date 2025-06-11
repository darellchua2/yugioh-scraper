import re
from typing import List
import pandas as pd
import unicodedata


from ..config import MEDIAWIKI_URL, HEADERS
from ..models.yugipedia_models import YugiohCard, YugiohSetCard, YugiohSet, YugiohRarity
from .misc_utilities import run_wiki_request_until_response


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
    Supports `noabbr`, entry-specific options, quantity, and description.
    """
    all_records = []

    for page_title, wikitext in wikitext_map.items():
        set_blocks = re.findall(r"{{Set list\|(.+?)}}", wikitext, re.DOTALL)

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
        # Debugging: print a snippet of the wikitext to check if it's correctly fetched
        print(f"Processing page: {page_title}")
        # Print a snippet of the wikitext to check if the data looks correct
        print(wikitext[:500])

        yugioh_set = next(
            (s for s in yugioh_sets if s.yugipedia_set_card_list == page_title), None)
        if yugioh_set is None:
            print(f"⚠️ Set not found: {page_title}")
            continue

        set_blocks = re.findall(r"{{Set list\|(.+?)}}", wikitext, re.DOTALL)
        for block in set_blocks:
            lines = block.strip().splitlines()
            global_params = {}
            entry_lines = []

            # Skip the first line (header) and process the remaining lines for card data
            for line in lines[1:]:  # Skip the header line
                entry_lines.append(line.strip())

            # Extract global parameters from the header (first line)
            header_line = lines[0].strip()  # This is the first line (header)
            global_params = {k.strip(): v.strip() for k, v in (param.split(
                '=', 1) for param in header_line.split('|') if '=' in param)}

            # Extract global rarity from header
            default_rarities = global_params.get("rarities", "").split(',')
            # Clean any spaces
            default_rarities = [r.strip()
                                for r in default_rarities if r.strip()]

            # If no rarity is defined in the header, we don't set any default rarity (do nothing here)
            if not default_rarities:
                print(f"⚠️ No global rarity found in header for {page_title}")

            for line in entry_lines:
                if not line.strip():
                    continue

                parts_main = line.split('//', 1)
                entry_data = parts_main[0].strip()
                entry_opts = parts_main[1].strip() if len(
                    parts_main) > 1 else ""

                # Check for alternate artwork in the description (after //)
                is_alternate_artwork = any(
                    marker in entry_opts.lower()
                    for marker in ("alternate artwork", "international artwork", "new artwork", "9th artwork", "8th artwork", "7th artwork")
                )

                # Split entry data into fields (card code, name, rarity, print code, quantity)
                fields = [p.strip() for p in entry_data.split(';')]

                # If there are fewer than 5 fields, fill the missing ones with defaults
                if len(fields) < 5:
                    print(f"⚠️ Incomplete data for entry: {line}")
                    # Fill missing fields with empty strings
                    fields += [""] * (5 - len(fields))
                    print(f"Fixed fields: {fields}")

                # Assign the fields, using defaults where necessary
                set_card_code, raw_name, raw_rarity, print_code, quantity = fields[:5]

                # Normalize the card name
                card_name = normalize_card_name(raw_name)
                print(f"Normalized card name: {card_name}")  # Debugging print

                # Handle card lookup by name
                yugioh_card = find_card_by_english_name(
                    card_name, yugioh_cards)
                if not yugioh_card:
                    print(f"⚠️ Card not found: {card_name}")
                    continue

                # Handle multiple rarities (split by comma)
                if raw_rarity:
                    rarities = raw_rarity.split(',')
                else:
                    # If no rarity is provided, use the global default rarity (from the header)
                    rarities = default_rarities

                rarity_list = [r.strip() for r in rarities] if rarities else []

                # Parse the entry options (e.g., description, other metadata)
                entry_opts_dict = {}
                for opt in re.split(r',|\s', entry_opts):
                    if '=' in opt:
                        k, v = opt.split('=', 1)
                        entry_opts_dict[k.strip()] = v.strip()

                # Iterate through the list of rarities and create YugiohSetCard objects
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
                        is_alternate_artwork=is_alternate_artwork  # Set alternate artwork flag
                    )
                    # Optionally store quantity or description if needed
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
