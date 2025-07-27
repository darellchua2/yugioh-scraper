import csv
import logging
import requests
from typing import List, Dict
from ...config import HEADERS, MEDIAWIKI_URL


class YugiohRarity:
    def __init__(self, name: str, pageid: str):
        self.name: str = name
        self.pageid: str = pageid

    def to_dict(self) -> Dict[str, str]:
        return {
            "name": self.name
        }


class YugiohRarityRedirect:
    def __init__(self, prefix: str, pageid: str, rarity: YugiohRarity):
        self.prefix: str = prefix
        self.pageid: str = pageid
        self.rarity: YugiohRarity = rarity

    def get_dict(self):
        return {
            "name": self.rarity.name,
            "prefix": self.prefix,
            "pageid": self.pageid
        }


def get_category_members(category_name: str, limit: int = 500) -> List[Dict[str, str]]:
    params = {
        "action": "query",
        "format": "json",
        "list": "categorymembers",
        "cmtitle": category_name,
        "cmnamespace": "0",
        "cmlimit": limit
    }

    response = requests.get(
        MEDIAWIKI_URL, headers=HEADERS, params=params, timeout=10)
    if response.status_code == 200:
        data = response.json()
        if "query" in data and "categorymembers" in data["query"]:
            return [{"title": member["title"], "pageid": member["pageid"]} for member in data["query"]["categorymembers"]]
        else:
            logging.info(f"No members found for category: {category_name}")
            return []
    else:
        logging.info(
            f"Failed to fetch category members: {response.status_code}")
        return []


def get_redirects_for_pages(titles: List[str], limit: int = 500) -> List[Dict[str, str]]:
    final_redirects = []
    params = {
        "action": "query",
        "format": "json",
        "prop": "redirects",
        "rdlimit": limit
    }

    for i in range(0, len(titles), 50):
        batch_titles = titles[i:i + 50]
        params["titles"] = "|".join(batch_titles)

        while True:
            response = requests.get(
                MEDIAWIKI_URL, headers=HEADERS, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                print("data", data)
                if "query" in data and "pages" in data["query"]:
                    pages_values: List[Dict] = data['query']["pages"].values()
                    for page_value in pages_values:
                        print("page_value", page_value)
                        if "redirects" in page_value.keys():
                            print("page_value", page_value)
                            # type: ignore
                            redirect_list: List[Dict] = page_value['redirects']
                            for redirect in redirect_list:
                                print("redirect", redirect)
                                final_redirects.append(
                                    {"prefix": redirect['title'], "rarity_name": page_value['title'], "pageid": redirect['pageid']})

                if "continue" in data:
                    params.update(data["continue"])
                else:
                    break
            else:
                print(
                    f"Failed to fetch redirects for batch: {batch_titles}: {response.status_code}")
                break

    return final_redirects


def save_rarity_redirects_to_csv(rarities: List[YugiohRarityRedirect], filename: str) -> None:
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['prefix', 'pageid',
                        'name', 'rarity_pageid'])
        for rarity_redirect in rarities:
            writer.writerow([
                rarity_redirect.prefix,
                rarity_redirect.pageid,
                rarity_redirect.rarity.name,
                rarity_redirect.rarity.pageid
            ])


def step_1_get_rarities_from_api() -> List[YugiohRarity]:
    rarities_name_dicts = get_category_members("Category:Rarities", limit=500)
    rarities: List[YugiohRarity] = []
    for rarity_name_dict in rarities_name_dicts:
        rarity = YugiohRarity(
            name=rarity_name_dict['title'], pageid=rarity_name_dict['pageid'])
        rarities.append(rarity)

    return rarities


def step_2_get_redirects_using_rarity_names(rarities: List[YugiohRarity]) -> List[YugiohRarityRedirect]:
    rarity_redirects: List[YugiohRarityRedirect] = []
    rarity_dict = {rarity.name: rarity for rarity in rarities}
    rarity_names = [key for key in rarity_dict.keys()]

    rarity_redirect_objs = get_redirects_for_pages(rarity_names)

    for rarity_redirect_obj in rarity_redirect_objs:
        matching_rarity = rarity_dict.get(rarity_redirect_obj["rarity_name"])
        if matching_rarity:
            rarity_redirects.append(YugiohRarityRedirect(
                prefix=rarity_redirect_obj["prefix"],
                pageid=rarity_redirect_obj["pageid"],
                rarity=matching_rarity
            ))

    return rarity_redirects


def get_yugioh_rarities_v2():
    rarities: List[YugiohRarity] = step_1_get_rarities_from_api()
    rarity_redirects: List[YugiohRarityRedirect] = step_2_get_redirects_using_rarity_names(
        rarities)

    save_rarity_redirects_to_csv(rarity_redirects, "yugioh_rarities.csv")
    return rarity_redirects


if __name__ == "__main__":
    get_yugioh_rarities_v2()
