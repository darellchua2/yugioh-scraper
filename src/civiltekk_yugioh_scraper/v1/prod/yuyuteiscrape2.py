import concurrent
import concurrent.futures
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from typing import Any
import requests
import bs4 as bs
from bs4 import BeautifulSoup, Tag
import pandas as pd
import datetime
from dotenv import load_dotenv
import re

from ..utilities.aws_utilities import upload_data


def get_rarity_mapping_dict() -> dict:
    rarity_dict = {
        '10000SE': '10000 Secret Rare',
        '20thSE': '20th Secret Rare',
        'CR': "Collector's Rare",
        'N': 'Common',
        'P-EXSE': 'Extra Secret Parallel Rare',
        'EXSE': 'Extra Secret Rare',
        'GR': 'Gold Rare',
        'GSE': 'Gold Secret Rare',
        'P-HR': 'Holographic Parallel Rare',
        'HR': 'Holographic Rare',
        'KC-N': 'Kaiba Corporation Common',
        'KC-R': 'Kaiba Corporation Rare',
        'KC-UR': 'Kaiba Corporation Ultra Rare',
        'M-GR': 'Millennium Gold Rare',
        'M': 'Millennium Rare',
        'M-SE': 'Millennium Secret Rare',
        'M-SR': 'Millennium Super Rare',
        'M-UR': 'Millennium Ultra Rare',
        'P-N': 'Normal Parallel Rare',
        'NR': 'Normal Rare',
        'PG': 'Premium Gold Rare',
        'PSE': 'Prismatic Secret Rare',
        'R': 'Rare',
        'P-SE': 'Secret Parallel Rare',
        'SE': 'Secret Rare',
        'P-SR': 'Super Parallel Rare',
        'SR': 'Super Rare',
        'UL': 'Ultimate Rare',
        'P-UR': 'Ultra Parallel Rare',
        'UR': 'Ultra Rare',
        'QCSE': 'Quarter Century Secret Rare',
        'SP': 'Super Rare',
        'ｼｰｸﾚｯﾄ': 'Secret Rare',
        'NP': 'Normal Parallel Rare',
        "SPECIAL RED": "Secret Rare Special Red Version"
    }
    return rarity_dict


def update_rarity_for_308():
    rarity_dict = {
        'PR': 'Ultra Parallel Rare'
    }

    return rarity_dict


def get_card_price2(x) -> str | float | None:
    pattern = re.compile(r'(.+) 円')  # \u5186 refers to 円
    match = pattern.search(x)
    if match:
        try:
            return float(match.group(1).replace(",", ""))
        except:
            return match.group(1)

    return None


def get_set_code(text_string: str):
    pattern = re.compile(r'\[(.+)\]')
    pattern_search = pattern.search(text_string)
    if pattern_search:
        return pattern_search.group(1)

    return None


def get_card_set_codes_from_card_set(obj: dict) -> list[dict]:
    list_needed: list = []
    try:
        url: str = obj['url']
        print('url:', url)
        set_code = obj['set_code']
        response = requests.get(url)
        response.encoding = 'utf-8'
        source = response.text
        soup = bs.BeautifulSoup(source, 'html.parser')

        rarity_divs = soup.find_all(id=re.compile("^(card-list).?"))

        for rarity_div in rarity_divs:
            rarity_span = rarity_div.find('span')
            rarity_code = None

            # search for rarity code
            if rarity_span:
                rarity_code = rarity_span.text

            card_divs = rarity_div.find_all(
                'div', "col-md")
            for card_div in card_divs:
                card_obj = {'card_rarity': rarity_code, 'set_code': set_code}
                set_card_code = ""
                jap_price = None
                set_card_name_jap = None

                set_card_code_span = card_div.find('span')
                if set_card_code_span:
                    set_card_code = set_card_code_span.text

                jap_price_strong = card_div.find('strong')
                jap_price_strong_text = jap_price_strong.text
                if jap_price_strong:
                    jap_price = get_card_price2(jap_price_strong_text)

                set_card_name_jap_h4 = card_div.find('h4')
                set_card_name_jap_h4_text = set_card_name_jap_h4.text

                if "（イラス" in set_card_name_jap_h4_text or "（イラ" in set_card_name_jap_h4_text or "(新規" in set_card_name_jap_h4_text or "(海外" in set_card_name_jap_h4_text:
                    set_card_code = set_card_code + "b"

                # ADD FOR QCAC Check
                if "(SPEC" in set_card_name_jap_h4_text and set_code == "QCAC":
                    rarity_code = "SPECIAL RED"

                card_obj['Price'] = jap_price
                card_obj['card_set_card_code'] = set_card_code
                card_obj['url'] = obj['url']
                if rarity_code is not None:  # to make sure that rarity code is not going to throw error
                    list_needed.append(card_obj.copy())

    except requests.ConnectionError as e:
        print(e)
    except Exception as e:
        print(e.args)
    return list_needed


# not in use anymore
def get_set_list(url: str) -> list[dict]:

    dict_list = []
    response = requests.get(url)
    response.encoding = 'utf-8'
    source = response.text
    soup = bs.BeautifulSoup(source, 'html.parser')
    div: bs.Tag | None = soup.find(
        'div', id='side-sell-single')
    if not div:
        return []

    inputs = div.find_all(
        "a", attrs={'id': re.compile(r'^(side-sell-ygo-s-).?')})

    for a in inputs:
        obj = {}

        a_text = a.text if a.text else None
        a_href = a['href'] if a['href'] else None
        if a_href:
            obj['url'] = a['href']
        if a_text:
            obj['set_code'] = get_set_code(a_text)
        dict_list.append(obj.copy())
    dict_list = add_additional_url(dict_list)
    seen_keys = {item['url'] for item in dict_list}
    unique_data = [next(item for item in dict_list if item['url'] == key)
                   for key in seen_keys]

    return unique_data


def get_set_list_v2(url: str) -> list[dict]:
    dict_list = []
    try:
        response = requests.get(url)
        response.encoding = 'utf-8'
        source = response.text
    except requests.RequestException as e:
        print(f"Error fetching URL: {e}")
        return []

    soup = BeautifulSoup(source, 'html.parser')
    div: Tag | None = soup.find('div', id='side-sell-single')
    if not div or not isinstance(div, Tag):
        print("Target div not found or is not a valid Tag")
        return []

    inputs = div.find_all(
        "a", attrs={'id': re.compile(r'^(side-sell-ygo-s-).?')})

    for a in inputs:
        obj = {}
        a_text = a.text.strip() if a.text else None
        a_href = a.get('href', None)
        if a_href:
            obj['url'] = a_href
        if a_text:
            obj['set_code'] = get_set_code(a_text)
        dict_list.append(obj.copy())

    dict_list = add_additional_url(dict_list)

    # Remove duplicates while preserving the first occurrence
    seen_keys = set()
    unique_data = []
    for item in dict_list:
        if item['url'] not in seen_keys:
            seen_keys.add(item['url'])
            unique_data.append(item)

    return unique_data


def add_additional_url(dict_list: list[dict]):
    dict_list.append({'url': 'https://yuyu-tei.jp/sell/ygo/s/search?search_word=qccu&page=2',
                      'set_code': 'QCCU'})
    dict_list.append({'url': 'https://yuyu-tei.jp/sell/ygo/s/search?search_word=qccp&page=2',
                      'set_code': 'QCCP'})
    return dict_list


def replace_dt_rarity_name(input_df: pd.DataFrame):
    # Define your regex condition
    regex_condition = "^DT.*"

    # Use the .str.match method to filter rows based on the regex condition
    filtered_rows = input_df[input_df['card_set_card_code'].str.match(
        regex_condition)]

    # Define your dictionary for replacing values in 'mapped_rarity' column
    mapping_dict = {
        "Rare": "Duel Terminal Rare Parallel Rare",
        "Common": "Duel Terminal Normal Parallel Rare",
        "Ultra Rare": 'Duel Terminal Ultra Parallel Rare',
        "Super Rare": 'Duel Terminal Super Parallel Rare',
        "Secret Rare": 'Duel Terminal Secret Parallel Rare',
        "Normal Rare": 'Duel Terminal Normal Rare Parallel Rare'
    }

    # Replace values in 'mapped_rarity' based on the dictionary
    filtered_rows['mapped_rarity'] = filtered_rows['mapped_rarity'].map(
        mapping_dict)

    # Update the original DataFrame with the modified values
    input_df.loc[filtered_rows.index,
                 'mapped_rarity'] = filtered_rows['mapped_rarity']

    return input_df


def yuyutei_scrape_old(dev_type=None):
    load_dotenv()
    start = datetime.datetime.now()

    print(start.strftime("%Y-%m-%d %H:%M:%S"))

    url2 = "https://yuyu-tei.jp/sell/ygo/s/search"

    card_set_obj_list: list[dict] = get_set_list_v2(url2)
    final_list: list[dict] = []

    # card_set_obj_list = [card_set_obj_list[0]]  # used t
    card_set_obj_list = [
        {'url': 'https://yuyu-tei.jp/sell/ygo/s/qccu', 'set_code': 'QCCU'}]  # used t

    with ThreadPoolExecutor(4) as executor:
        futures: list = []

        for obj in card_set_obj_list:
            futures.append(executor.submit(
                get_card_set_codes_from_card_set, obj))

        for future in concurrent.futures.as_completed(futures):
            try:
                future_list = future.result()
                final_list.extend(future_list)
            except Exception as e:
                print(e)
                continue

    df = pd.DataFrame(final_list)
    print(df.columns)
    print("shape", df.shape)
    rarity_dict = get_rarity_mapping_dict()
    df["mapped_rarity"] = df["card_rarity"].map(rarity_dict)
    df['date'] = datetime.datetime.now()

    # drop card_carity column
    df = df.drop(columns=['card_rarity'])

    df = df.dropna(subset=['mapped_rarity', 'card_set_card_code'])
    df = replace_dt_rarity_name(df)

    buffer = BytesIO()
    df.to_csv(buffer, index=False)

    df = df[['Price', 'card_set_card_code', 'mapped_rarity', 'date']]
    upload_data(df, 'yuyutei', 'append', 'yugioh_data')
    upload_data(df, 'yuyutei_latest', 'replace', 'yugioh_data')

    end = datetime.datetime.now()
    print(start.strftime("%Y-%m-%d %H:%M:%S"))
    print(end.strftime("%Y-%m-%d %H:%M:%S"))
    difference = end - start
    print(f"The time difference between the 2 time is: {difference}")


def yuyutei_scrape(dev_type=None):
    load_dotenv()
    start = datetime.datetime.now()

    print(start.strftime("%Y-%m-%d %H:%M:%S"))

    url2 = "https://yuyu-tei.jp/sell/ygo/s/search"

    card_set_obj_list: list[dict] = get_set_list_v2(url2)
    final_list: list[dict] = []

    # card_set_obj_list = [
    #     {'url': 'https://yuyu-tei.jp/sell/ygo/s/qccu', 'set_code': 'QCCU'}]  # used t

    with ThreadPoolExecutor(4) as executor:
        futures: list = []

        for obj in card_set_obj_list:
            futures.append(executor.submit(
                get_card_set_codes_from_card_set, obj))

        for future in concurrent.futures.as_completed(futures):
            try:
                future_list = future.result()
                final_list.extend(future_list)
            except Exception as e:
                print(e)
                continue

    # Check if final_list is empty
    if not final_list:
        print("No data found. Exiting the function.")
        return  # Exit the function early

    df = pd.DataFrame(final_list)
    print(df.columns)
    print("shape", df.shape)

    rarity_dict = get_rarity_mapping_dict()
    df["mapped_rarity"] = df["card_rarity"].map(rarity_dict)
    df['date'] = datetime.datetime.now()

    # Drop card_carity column
    df = df.drop(columns=['card_rarity'])

    # Drop rows with missing values in key columns
    df = df.dropna(subset=['mapped_rarity', 'card_set_card_code'])

    df = replace_dt_rarity_name(df)

    buffer = BytesIO()
    df.to_csv(buffer, index=False)

    df = df[['Price', 'card_set_card_code', 'mapped_rarity', 'date']]
    upload_data(df, 'yuyutei', 'append', 'yugioh_data')
    upload_data(df, 'yuyutei_latest', 'replace', 'yugioh_data')

    end = datetime.datetime.now()
    print(start.strftime("%Y-%m-%d %H:%M:%S"))
    print(end.strftime("%Y-%m-%d %H:%M:%S"))
    difference = end - start
    print(f"The time difference between the 2 times is: {difference}")


if __name__ == "__main__":
    yuyutei_scrape()
