from __future__ import annotations
import json
from typing import Any, Dict, List, Optional
import re
import ast


def format_lore(value: Optional[str]) -> str:
    """
    Formats the lore text by replacing HTML break tags with newlines
    and processing internal wiki link syntax.
    If the input value is None, returns an empty string.
    """
    if not value:
        return ""  # Return an empty string if value is None or empty

    # Replace <br /> tags with newlines
    value_string = value.replace("<br />", "\n")

    # Regex to find internal wiki link patterns
    pattern_2 = r"\[{2}(.*?\|?.*?)\]{2}"
    matches = re.findall(pattern_2, value_string)

    for match in matches:
        # Split on the pipe '|' and take the first part if present
        processed_text = match.split("|")[0].strip()
        # Replace the full match (with [[ and ]]) in the original string
        value_string = value_string.replace(f"[[{match}]]", processed_text)

    return value_string


class YugiohSet:
    def __init__(self,
                 name: str | None = None,
                 set_code: str | None = None,
                 set_image: str | None = None,
                 language: str | None = None,
                 card_game: str | None = None,
                 release_date: str | None = None,
                 image_file: str | None = None,
                 image_url: str | None = None,
                 region: str | None = None,
                 ):
        self.name = name
        self.set_code = set_code
        self.language = language
        self.set_image = set_image
        self.release_date = release_date
        self.image_file = image_file
        self.image_url = image_url
        self.region = region

        if not card_game:
            if language and language in ("JP", "KR", "JA", "AE"):
                self.card_game = "OCG"
            if language and language in ("EN"):
                self.card_game = "TCG"
        else:
            self.card_game = card_game

        self.yugipedia_set_card_list = "Set Card Lists:{name} ({card_game}-{language})".format(
            name=self.name, card_game=self.card_game, language=self.language).replace(" (OCG)", "").replace(" (set)", "")

        self.yugipedia_set_card_gallery = "Set Card Galleries:{name} ({card_game}-{language})".format(
            name=self.name, card_game=self.card_game, language=self.language).replace(" (OCG)", "").replace(" (set)", "")

        yugipedia_set_card_gallery_url = "https://yugipedia.com/wiki/{gallery}".format(
            gallery=self.yugipedia_set_card_gallery)

        self.yugipedia_set_card_gallery_url = yugipedia_set_card_gallery_url
        self.yugipedia_set_card_list_url = "https://yugipedia.com/wiki/{list_name}".format(
            list_name=self.yugipedia_set_card_list)

    def get_dict(self):
        return self.__dict__

    def get_yugipedia_dict(self) -> dict:
        obj = {}

        return self.__dict__

    @classmethod
    def get_yugioh_set_from_db_obj(cls, obj: dict) -> YugiohSet:
        return YugiohSet(
            name=obj.get('name'),
            set_code=obj.get('set_code'),
            set_image=obj.get('set_image'),
            language=obj.get('language'),
            card_game=obj.get('card_game'),
            release_date=obj.get('release_date'),
            region=obj.get('region')
        )


class YugiohCard:
    """
    A class to represent a Yu-Gi-Oh! card with relevant details.
    """

    def __init__(self, name: str, attributes: Dict[str, Any]) -> None:
        self.name: str = name or ""
        self.english_name: str = self.get_first(
            attributes.get("English name", attributes.get("english_name", name)))
        self.password: str | None = self.get_first(
            attributes.get("Password", attributes.get("password", None)))
        self.card_type: str = self.extract_fulltext_single(
            attributes.get("Card type", attributes.get("card_type", None)))
        self.level: str = self.get_first(
            attributes.get("Level", attributes.get("level", None)))
        self.race: str = self.extract_fulltext_single(
            attributes.get("Primary type", attributes.get("race", None)))
        self.type: str = self.extract_fulltext_single(attributes.get("Type"))
        self.archetype_support = self.extract_fulltext_single(
            attributes.get("Archetype support", attributes.get("archetype_support", None)))
        self.archetypes: str = self.stringify_list(
            self.extract_fulltext_list(attributes.get("Archseries", attributes.get("archetypes"))))
        self.property: str = self.get_first(attributes.get(
            "Property", attributes.get("property", None)))
        self.lore: str = format_lore(self.get_first(
            attributes.get("Lore", attributes.get("lore", None))))
        self.attribute: str = self.extract_fulltext_single(
            attributes.get("Attribute", attributes.get("attribute", None)))
        self.atk_value: str = self.get_first(
            attributes.get("ATK", attributes.get("atk_value", None)))
        self.def_value: str = self.get_first(
            attributes.get("DEF", attributes.get("def_value", None)))
        self.atk_string: str = self.get_first(attributes.get(
            "ATK string", attributes.get("atk_string", None)))
        self.def_string: str = self.get_first(attributes.get(
            "DEF string", attributes.get("def_string", None)))
        self.link_arrows: str = self.stringify_list(
            self.get_link_arrows(attributes.get("Link Arrows", attributes.get("link_arrows", "[]"))))
        self.link_rating: str = self.get_first(attributes.get(
            "Link Rating", attributes.get("link_rating", None)))
        self.materials: str = self.get_first(attributes.get(
            "Materials", attributes.get("materials", None)))
        self.pendulum_scale: str = self.get_first(
            attributes.get("Pendulum Scale", attributes.get("pendulum_scale", None)))
        self.pendulum_effect: str = format_lore(
            self.get_first(attributes.get("Pendulum Effect", ""))) if self.get_first(attributes.get("Pendulum Effect")) else attributes.get("pendulum_effect", "")
        self.rank: str = self.get_first(
            attributes.get("Rank", attributes.get("rank", None)))
        self.ocg_status: str = self.extract_fulltext_single(
            attributes.get("OCG status", attributes.get("ocg_status", None)))
        self.card_image_name: str = self.get_first(
            attributes.get("Card image name", attributes.get("card_image_name", None)))
        self.release: str = self.extract_fulltext_single(
            attributes.get("Release", attributes.get("release", None)))

    def get_link_arrows(self, value: Optional[Any]) -> List[str]:
        """
        Processes the 'Link Arrows' attribute, returning a list of strings if present,
        or an empty list if no link arrows are found.
        """
        if isinstance(value, list) and len(value) > 0:
            print(value)
            return [entry for entry in value]
        return []

    def get_first(self, value: Optional[Any]) -> str:
        """
        Helper method to extract the first value if the attribute is a list,
        otherwise return the value as a string or an empty string.
        """
        if isinstance(value, list) and value:
            return str(value[0])
        return str(value) if value else ""

    def extract_fulltext_single(self, value: Optional[Any]) -> str:
        """
        Extracts the 'fulltext' field from a single value if it exists.
        Otherwise, returns an empty string.
        """
        if isinstance(value, list) and value and isinstance(value[0], dict) and "fulltext" in value[0]:
            return value[0]["fulltext"]
        return ""

    def extract_fulltext_list(self, value: Optional[Any]) -> List[str]:
        """
        Extracts the 'fulltext' field from a list of values, returning a list of strings.
        If the value is not a list or no 'fulltext' fields exist, returns an empty list.
        """
        if isinstance(value, list):
            return [entry["fulltext"] for entry in value if isinstance(entry, dict) and "fulltext" in entry]
        return []

    def stringify_list(self, values: List[str]) -> str:
        """
        Converts a list of strings into a JSON-like stringified array.
        """
        return json.dumps(values)

    def __str__(self) -> str:
        """
        Return a string representation of the card.
        """
        return (
            f"Name: {self.name}\n"
            f"Type: {self.card_type}\n"
            f"Attribute: {self.attribute}\n"
            f"ATK: {self.atk_value}\n"
            f"DEF: {self.def_value}\n"
            f"Archetypes: {self.archetypes}\n"
            f"Link Arrows: {self.link_arrows}\n"
            f"Lore: {self.lore}\n"
        )

    @classmethod
    def get_yugioh_card_from_db_obj(cls, attributes: dict):
        return YugiohCard(
            name=attributes.get("name", ""),
            attributes=attributes
        )

    @classmethod
    def get_yugipedia_dict_from_yugioh_card(cls, card: YugiohCard):
        if isinstance(card, YugiohCard):
            desc = "[ Pendulum Effect ]" + "\n" + card.pendulum_effect + \
                "\n" + card.lore if card.pendulum_effect is not None else card.lore

            yugipedia_obj = {
                "name": card.name,
                "id": card.password,
                "level": card.level if card.level is not None else card.rank if card.rank is not None else None,
                "desc": desc,
                "archetype": ast.literal_eval(card.archetypes)[0] if len(ast.literal_eval(card.archetypes)) > 0 else None,
                "race": card.race.replace(" Card", "") if card.race is not None else None,
                "type": card.card_type,
                "atk": card.atk_string,
                "def": card.def_string,
                "attribute": card.attribute,
                "linkval": card.link_rating,
                "linkmarkers": ast.literal_eval(card.link_arrows)[0] if len(ast.literal_eval(card.link_arrows)) > 0 else None,
                "scale": card.pendulum_scale,
                "card_image_name": card.card_image_name
            }

            return yugipedia_obj

    def get_dict(self):
        self.archetypes = str(self.archetypes)
        self.link_arrows = str(self.link_arrows)
        return self.__dict__


class YugiohRarity:
    def __init__(self, name: str, prefix: str, pageid: int):
        self.name: str = name
        self.prefix: str = prefix
        self.pageid: int = pageid

    def get_dict(self) -> dict:
        return self.__dict__

    @classmethod
    def get_yugioh_rarity_from_db_obj(cls, obj: dict) -> YugiohRarity:
        return YugiohRarity(
            name=obj.get('name', ""),
            prefix=obj.get('prefix', ""),
            pageid=obj.get('pageid', 0)
        )

    def __repr__(self):
        return f"YugiohRarity(name={self.name}, prefix={self.prefix}, pageid={self.pageid})"


class YugiohSetCard:
    def __init__(self,
                 yugioh_set: YugiohSet | None,
                 yugioh_card: YugiohCard | None,
                 yugioh_rarity: YugiohRarity | None,
                 code: str | None = None,
                 image_url: str | None = None,
                 image_file: str | None = None,
                 is_alternate_artwork: bool = False
                 ):
        if isinstance(yugioh_set, YugiohSet):
            self.set = yugioh_set
        else:
            self.set = None
        if isinstance(yugioh_card, YugiohCard):
            self.card = yugioh_card
        else:
            self.card = None
        if isinstance(yugioh_rarity, YugiohRarity):
            self.rarity = yugioh_rarity
        else:
            self.rarity = None
        self.code: str | None = code
        self.image_url: str | None = image_url
        self.image_file: str | None = image_file
        self.is_alternate_artwork: bool = is_alternate_artwork

    def get_dict(self) -> dict:
        return self.__dict__

    def get_dict_from_yugioh_set_card(self) -> dict | None:
        if self.card and self.set and self.rarity:
            obj = {
                "card_name": self.card.name,
                "card_code": self.code,
                "card_rarity": self.rarity.name,
                "card_set": self.set.name,
                "image_url": self.image_url,
                "image_file": self.image_file,
                "is_alternate_artwork": self.is_alternate_artwork,
            }
            return obj
        else:
            card_name = self.card.name if self.card is not None else None
            card_rarity = self.rarity.name if self.rarity is not None else None
            card_set = self.set.name if self.set is not None else None
            obj = {
                "card_name": card_name,
                "card_code": self.code,
                "card_rarity": card_rarity,
                "card_set": card_set,
                "image_url": self.image_url,
                "image_file": self.image_file,
                "is_alternate_artwork": self.is_alternate_artwork,
            }
            return obj

    def get_tekkx_wordpress_dict_from_yugioh_set_card(self) -> dict | None:
        if self.card and self.set and self.rarity:
            desc: str = self.card.lore

            english_name_wordpress: str = self.card.english_name if not self.is_alternate_artwork else self.card.name + \
                (" (alternate art)" if self.is_alternate_artwork else None)  # type: ignore
            set_card_code_updated: str = self.code + \
                "b" if self.is_alternate_artwork and self.code else self.code  # type: ignore

            archetypes_list = ast.literal_eval(
                self.card.archetypes.strip())

            obj = {
                "set_card_name_combined": english_name_wordpress,
                "set_card_code_updated": set_card_code_updated,
                "card_name": self.card.name,
                "english_name": self.card.english_name,
                "rarity_name": self.rarity.name,
                "set_name": self.set.name,
                "set_card_code": self.code,
                "id": self.card.password,
                "level": self.card.level if self.card.level is not None else self.card.rank if self.card.rank is not None else None,
                "region": self.set.region,
                "language": self.set.language,
                "desc": desc,
                "archetype": archetypes_list[0] if len(archetypes_list) > 0 else None,
                "race": self.card.race,
                "atk": self.card.atk_string,
                "def": self.card.def_string,
                "type": self.card.card_type,
                "attribute": self.card.attribute,
                "linkval": self.card.link_rating,
                "linkmarkers": str(self.card.link_arrows) if len(self.card.link_arrows) > 0 else None,
                "scale": self.card.pendulum_scale,
                "image_url": self.image_url,
                "image_file": self.image_file,
                "is_alternate_artwork": self.is_alternate_artwork,
                "lore": self.card.lore,
                "pendulum_effect": self.card.pendulum_effect,
                "archetypes": self.card.archetypes,
                "link_arrows": self.card.link_arrows,
                "card_image_name": self.card.card_image_name
            }
            return obj
        else:
            return None
