from __future__ import annotations
import re


class BigwebSetCard:
    def __init__(self,
                 id: str | None = None,
                 name: str | None = None,
                 fname: str | None = None,
                 image: str | None = None,
                 stock_count: int | None = None,
                 condition: BigwebSetCardCondition | None = None,
                 price: float | None = None,
                 sale_prices: float | None = None,
                 rarity: BigwebRarity | None = None,
                 bigweb_set: BigwebSet | None = None,
                 date_updated=None
                 ):
        self.id: str | None = id
        self.name: str | None = name
        self.fname: str | None = fname
        self.image: str | None = image
        self.stock_count = stock_count
        self.price = price
        self.sale_prices = sale_prices
        self.date_updated = date_updated
        self.condition = None
        self.rarity = None
        self.set = None

        if condition and isinstance(condition, BigwebSetCardCondition):
            self.condition = condition

        if rarity and isinstance(rarity, BigwebRarity):
            self.rarity = rarity

        if bigweb_set and isinstance(bigweb_set, BigwebSet):
            self.set = bigweb_set

        self.is_alternate_artwork = "international artwork" if self.fname and "海外イラスト" in self.fname else None

        self.yugipedia_set_card_code = None
        self.yugipedia_set_card_name = replace_cardname(self.name)

        card_code = self.fname.replace("*", "") if self.fname else None
        pattern_base_art = re.compile(r"^(.{2,4}-.{2,5})a$")
        pattern_set_code = re.compile(r"^(.{2,4})-(.{2,5})$")
        pattern_space = re.compile(r"^(.{2,4}-.{2,5})[ ]+$")
        pattern_set_code_v2 = re.compile(r"(.{2,4})-(.{2,5})$")
        if card_code is not None:
            is_pattern_base_art_match = pattern_base_art.match(card_code)
            is_pattern_space_match = pattern_space.match(card_code)
            is_pattern_set_code_match = pattern_set_code.match(card_code)
            is_pattern_set_code_v2_match = pattern_set_code_v2.match(card_code)
            if is_pattern_base_art_match:
                card_code = is_pattern_base_art_match.group(1)
            if is_pattern_space_match:
                card_code = is_pattern_space_match.group(1)
            if is_pattern_set_code_match:
                self.yugipedia_set_card_code = card_code
            else:
                if self.set and self.set.yugipedia_set_prefix:
                    self.yugipedia_set_card_code = self.set.yugipedia_set_prefix + "-" + card_code
            if is_pattern_set_code_v2_match:
                self.yugipedia_set_card_code = is_pattern_set_code_v2_match.group(
                    1) + "-" + is_pattern_set_code_v2_match.group(2)

    def get_dict(self):

        yugipedia_set_prefix = self.set.yugipedia_set_prefix if self.set else None

        pattern_set_code = re.compile(r"^(.{2,4})-(.{2,5})$")
        pattern_set = re.compile(r"^(.{2,4})$")

        # Ensure that self.yugipedia_set_card_code is not None and is a string
        if isinstance(self.yugipedia_set_card_code, str):
            match = pattern_set_code.match(self.yugipedia_set_card_code)
            if match:
                set_slip = str(self.set.slip) if self.set else ""
                if not pattern_set.match(set_slip):
                    yugipedia_set_prefix = match.group(1)
                elif set_slip != match.group(1):
                    yugipedia_set_prefix = match.group(1)
        else:
            print(
                f"Warning: yugipedia_set_card_code is of type {type(self.yugipedia_set_card_code)} and value {self.yugipedia_set_card_code}")
        obj = {
            "id": self.id,
            "name": self.name,
            "fname": self.fname,
            "image": self.image,
            "stock_count": self.stock_count,
            "price": self.price,
            "sale_prices": self.sale_prices,
            "cardset_web": self.set.web if self.set else None,
            "cardset_id": self.set.id if self.set else None,
            "cardset_slip": self.set.slip if self.set else None,
            "date_updated": self.date_updated,
            "condition_slip": self.condition.slip if self.condition else None,
            "yugipedia_set_prefix": yugipedia_set_prefix,
            "yugipedia_set_card_code": self.yugipedia_set_card_code,
            "yugipedia_rarity_prefix": self.rarity.yugipedia_rarity if self.rarity else None
        }
        return obj

    def get_yugioh_data_dict(self):

        yugipedia_set_prefix = self.set.yugipedia_set_prefix if self.set else None

        pattern_set_code = re.compile(r"^(.{2,4})-(.{2,5})$")
        pattern_set = re.compile(r"^(.{2,4})$")
        if isinstance(self.yugipedia_set_card_code, str):
            match = pattern_set_code.match(self.yugipedia_set_card_code)
            if match:
                # Extract group(1) once to avoid multiple matching calls
                yugipedia_set_prefix = match.group(1)
                set_slip = str(self.set.slip) if self.set else ""
                # Handle logic regarding self.set.slip
                if not pattern_set.match(set_slip):
                    # No changes needed here, just reuse the matched group
                    yugipedia_set_prefix = yugipedia_set_prefix
                elif set_slip != yugipedia_set_prefix:
                    # Assign again if slip is not equal to the prefix
                    yugipedia_set_prefix = yugipedia_set_prefix
        else:
            print(
                f"Warning: yugipedia_set_card_code is of type {type(self.yugipedia_set_card_code)} and value {self.yugipedia_set_card_code}")
        obj = {
            "card_set": yugipedia_set_prefix,
            "card_code": self.yugipedia_set_card_code,
            "jp_price": self.price,
            "mapped_rarity": self.rarity.yugipedia_rarity if self.rarity else None,
            "date": self.date_updated
        }

        return obj


class BigwebSetCardCondition:
    def __init__(self,
                 id=None,
                 web=None,
                 slip=None,
                 type=None,
                 ordering_id=None,
                 name=None
                 ):
        self.id = id
        self.web = web
        self.slip = slip
        self.type = type
        self.ordering_id = ordering_id
        self.name = name

    def get_dict(self):
        return self.__dict__


class BigwebSet:
    set_dict = {
        "SECRETSHINYBOX": "SSB1",
        "ALBASTRIKE": "SD43",
        "ﾓﾝｽﾄｺﾗﾎﾞ": "MSC1",
        "ﾌｨｷﾞｭｱｺﾚｸｼｮﾝ": "MFC1",
        "Ver.ライトニングスター": "DS13",
        "Ver.マシンギア": "DS14",
        "Ver.ライトロード": "DS14",
        "EXPERT 4": "EE04",
        "EXPERT 3": "EE3",
        "EXPERT 2": "EE2",
        "EXPERT 1": "EE1",
        "Ver.ダークリターナー": "DS13",
        "GOLD SERIES": "GS01",
        "PGB": "PGB1",
        "GDB": "GDB1",
        "【第2期】PH": "PH",
        "【第2期】MR": "MR",
        "SECRET UTILITY BOX": "SUB1",
        "PREMIUM PACK 23": "23PP",
        "ULTIMATE KAIBA SET": "KC01",
    }

    def __init__(self,
                 id=None,
                 web=None,
                 slip: str | None = None,
                 type=None,
                 ordering_id=None,
                 desc=None,
                 cardset_id=None,
                 code=None,
                 is_reservation=None,
                 is_box=None,
                 rack_number=None,
                 rack_ordering=None,
                 picking_type=None,
                 release=None
                 ):
        self.id: str | None = id
        self.web: str | None = web
        self.slip: str | None = slip
        self.type: str | None = type
        self.ordering_id = ordering_id
        self.desc = desc
        self.cardset_id = cardset_id
        self.code = code
        self.is_reservation = is_reservation
        self.is_box = is_box
        self.rack_number = rack_number
        self.rack_ordering = rack_ordering
        self.picking_type = picking_type
        self.release = release

        yugipedia_set_prefix: str | None = self.slip

        if yugipedia_set_prefix is not None:
            yugipedia_set_prefix = replace_the_word_new_in_set(
                yugipedia_set_prefix)
            yugipedia_set_prefix = replace_cardset_name(yugipedia_set_prefix)
            yugipedia_set_prefix = self.set_dict[yugipedia_set_prefix] if yugipedia_set_prefix in self.set_dict.keys(
            ) else yugipedia_set_prefix

        self.yugipedia_set_prefix = yugipedia_set_prefix

    def get_dict(self):
        return self.__dict__


class BigwebRarity:
    rarity_dict = {
        '10000シークレット': '10000 Secret Rare',
        '20SC': '20th Secret Rare',
        'CR': "Collector's Rare",
        'N': 'Common',
        'EXSP': 'Extra Secret Parallel Rare',
        'EXSR': 'Extra Secret Rare',
        'GR': 'Gold Rare',
        'GSR': 'Gold Secret Rare',
        'HL': 'Holographic Rare',
        'KC': 'Kaiba Corporation Common',
        'KC-Rare': 'Kaiba Corporation Rare',
        'KC-Ultra': 'Kaiba Corporation Ultra Rare',
        'Mil-Gold': 'Millennium Gold Rare',
        'Mil': 'Millennium Rare',
        'Mil-Secret': 'Millennium Secret Rare',
        'Mil-Super': 'Millennium Super Rare',
        'Mil-Ultra': 'Millennium Ultra Rare',
        'NP': 'Normal Parallel Rare',
        'NR': 'Normal Rare',
        'PG': 'Premium Gold Rare',
        'PSR': 'Prismatic Secret Rare',
        'R': 'Rare',
        'SCPR': 'Secret Parallel Rare',
        'SCR': 'Secret Rare',
        'SPPR': 'Super Parallel Rare',
        'SP': 'Super Rare',
        'RE': 'Ultimate Rare',
        'UPR': 'Ultra Parallel Rare',
        'UR': 'Ultra Rare',
        'P-N': 'Normal Parallel Rare',
        'ｼｰｸﾚｯﾄ': 'Secret Rare',
        'ｱﾙﾃｨﾒｯﾄ': 'Ultimate Rare',
        'Ｇ': 'Gold Rare',
        'ｼﾞｬﾝﾌﾟﾌｪｽﾀ': 'Common',
        'ステンレス': 'Ultra Rare',
        'PR': 'Ultra Parallel Rare',
        "US": 'Ultra Secret Rare',
        "10000SC": '10000 Secret Rare',
        'QCS(25th)': 'Quarter Century Secret Rare',
        'PSC': 'Prismatic Secret Rare',
        'ESP': 'Extra Secret Parallel Rare',
        'ES': 'Extra Secret Rare'
    }

    def __init__(self,
                 id=None,
                 web=None,
                 slip=None,
                 type=None,
                 ordering_id=None
                 ):

        self.id = id
        self.web = web
        self.slip = slip
        self.type = type
        self.ordering_id = ordering_id
        self.yugipedia_rarity: str | None = self.rarity_dict[slip] if isinstance(slip, str) and slip in self.rarity_dict.keys(
        ) else None

    def get_dict(self):
        return self.__dict__


def replace_cardname(x):
    pattern1 = re.compile("^《(.*)》$")
    is_pattern1_match = pattern1.match(x)
    if is_pattern1_match is not None:
        return is_pattern1_match.group(1)

    return x


def replace_the_word_new_in_set(x: str) -> str:
    pattern1 = re.compile("^.?【NEW】(.*)$")
    is_pattern1_match = pattern1.match(x)

    if is_pattern1_match:
        return is_pattern1_match.group(1)
    return x


def replace_cardset_name(x: str) -> str:
    pattern1 = re.compile(r"^.?\【JP[Y|M]\】(\w{4}).*$")
    pattern2 = re.compile(r"^.?【(\w{2,4})】.*$")
    pattern3 = re.compile(r"^.?\【\w*\】(\w{2,4})$")
    pattern5 = re.compile(r"^.?\【(\w[^第]*)\】.*$")
    pattern6 = re.compile(r"^.?\[(\w*)\].*$")
    pattern7 = re.compile(r"^(\w{1,4})-.*$")
    pattern8 = re.compile(r"^.?[\[|［](.*)-.*[］|\]].*")
    pattern9 = re.compile(r"^.?【[第].*】(.*)$")

    is_pattern1_match = pattern1.match(x)
    is_pattern2_match = pattern2.match(x)
    is_pattern3_match = pattern3.match(x)
    is_pattern5_match = pattern5.match(x)
    is_pattern6_match = pattern6.match(x)
    is_pattern7_match = pattern7.match(x)
    is_pattern8_match = pattern8.match(x)
    is_pattern9_match = pattern9.match(x)

    # for 【第2期】MR
    if is_pattern9_match:
        return is_pattern9_match.group(1)
    if is_pattern1_match:
        return is_pattern1_match.group(1)

    if is_pattern2_match:
        return is_pattern2_match.group(1)

    if is_pattern3_match:
        return is_pattern3_match.group(1)
    if is_pattern5_match:
        return is_pattern5_match.group(1)
    if is_pattern6_match:
        return is_pattern6_match.group(1)
    if is_pattern7_match:
        return is_pattern7_match.group(1)
    if is_pattern8_match:
        return is_pattern8_match.group(1)

    return x
