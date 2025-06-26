import sys
import os
import datetime
import requests
import re
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import concurrent

from ..utilities.misc_utilities import check_for_jap_chars, run_request_until_response
from ..utilities.aws_utilities import upload_data
from ..models.bigweb_models import BigwebRarity, BigwebSet, BigwebSetCard, BigwebSetCardCondition
from dotenv import load_dotenv


def get_rarity_mapping_dict():
    rarity_dict = {
        '10000SE': '10000 Secret Rare',
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
        'Ｇ': 'Gold Rare',
        'ｼﾞｬﾝﾌﾟﾌｪｽﾀ': 'Common',
        'ステンレス': 'Ultra Rare',
        'PR': 'Ultra Parallel Rare',
        "US": 'Ultra Secret Rare',
        'QCS(25th)': 'Quarter Century Secret Rare',
        'ES': 'Extra Secret Rare',
    }

    return rarity_dict


def replace_the_word_new_in_set(x: str):
    pattern1 = re.compile("^.?【NEW】(.*)$")
    if not isinstance(x, str):
        return x
    match1 = pattern1.match(x)
    if match1:
        return match1.group(1)
    return x


def get_bigweb_objs2(page_number):
    bigweb_set_cards = []
    final_obj = {}

    final_obj["bigweb_set_cards"] = bigweb_set_cards
    params = {"page_number": page_number}
    url_to_scrape = "https://api.bigweb.co.jp/products?game_id=9&page={page}".format(
        page=page_number)

    try:
        response = run_request_until_response(url=url_to_scrape, params=params)

        if response:
            response_dict = response.json()
            if response_dict:
                items = response_dict['items']
                for item in items:
                    rarity_obj = item['rarity']
                    bigweb_rarity = BigwebRarity(
                        id=rarity_obj['id'],
                        web=rarity_obj['web'],
                        slip=rarity_obj['slip'],
                        type=rarity_obj['type'],
                        ordering_id=rarity_obj['ordering_id']
                    )

                    condition_obj = item['card_condition']
                    if not condition_obj:
                        continue

                    bigweb_condition = BigwebSetCardCondition(
                        id=condition_obj['id'],
                        web=condition_obj['web'],
                        slip=condition_obj['slip'],
                        type=condition_obj['type'],
                        ordering_id=condition_obj['ordering_id'],
                        name=condition_obj['name']
                    )

                    cardset_obj = item['cardset']
                    bigweb_set = BigwebSet(
                        id=cardset_obj['id'],
                        web=cardset_obj['web'],
                        slip=cardset_obj['slip'],
                        type=cardset_obj['type'],
                        ordering_id=cardset_obj['ordering_id'],
                        desc=cardset_obj['desc'],
                        cardset_id=cardset_obj['cardset_id'],
                        code=cardset_obj['code'],
                        is_reservation=cardset_obj['is_reservation'],
                        is_box=cardset_obj['is_box'],
                        rack_number=cardset_obj['rack_number'],
                        rack_ordering=cardset_obj['rack_ordering'],
                        picking_type=cardset_obj['picking_type'],
                        release=cardset_obj['release']
                    )

                    bigweb_set_card = BigwebSetCard(
                        id=item['id'],
                        name=item['name'],
                        fname=item['fname'],
                        image=item['image'],
                        stock_count=item['stock_count'],
                        condition=bigweb_condition,
                        price=item['price'],
                        sale_prices=item['sale_prices'],
                        rarity=bigweb_rarity,
                        bigweb_set=bigweb_set,
                        # date_updated=date_updated
                    )

                    set_card_check = next(
                        (set_card for set_card in bigweb_set_cards if set_card.id == bigweb_set_card.id), None)
                    if not set_card_check:
                        bigweb_set_cards.append(bigweb_set_card)

            final_obj["bigweb_set_cards"] = bigweb_set_cards
        else:
            print("Response is None")
            return final_obj
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        if exc_tb is not None:
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print("Page {page_number}:".format(page_number=page_number),
                  exc_type, fname, exc_tb.tb_lineno)

    return final_obj


def bigweb_scrape():
    load_dotenv()
    start = datetime.datetime.now()

    URL = 'https://api.bigweb.co.jp/products?game_id=9'
    response = requests.get(URL, timeout=10)
    response_dict = response.json()
    total_page_to_iterate = response_dict['pagenate']['pageCount']
    print("total page count: {total_page_to_iterate}".format(
        total_page_to_iterate=total_page_to_iterate))

    final_bigweb_sets: list[BigwebSet] = []
    final_bigweb_set_cards: list[BigwebSetCard] = []
    final_bigweb_rarities: list[BigwebRarity] = []
    final_bigweb_conditions: list[BigwebSetCardCondition] = []

    # optimally defined number of threads
    with ThreadPoolExecutor(4) as executor:
        futures = []
        # total_page_to_iterate = 2  # for debugging
        for page_number in range(0, total_page_to_iterate + 1):
            futures.append(executor.submit(get_bigweb_objs2, page_number))
        for future in concurrent.futures.as_completed(futures):
            try:
                bigweb_set_cards = future.result()['bigweb_set_cards']
                final_bigweb_set_cards.extend(bigweb_set_cards)

            except AttributeError as e:
                print(type(e), e.args)

    date_updated = datetime.datetime.now()

    for index, bw_set_card in enumerate(final_bigweb_set_cards.copy()):
        bw_set_card.date_updated = date_updated
        final_bigweb_set_cards[index] = bw_set_card

    for bw_set_card in final_bigweb_set_cards:
        set_check = next(
            (bw_set for bw_set in final_bigweb_sets if bw_set_card.set and bw_set and bw_set.id == bw_set_card.set.id), None)
        if not set_check and bw_set_card.set:
            final_bigweb_sets.append(bw_set_card.set)

    for bw_set_card in final_bigweb_set_cards:
        rarity_check = next(
            (bw_rarity for bw_rarity in final_bigweb_rarities if (bw_set_card.rarity and bw_rarity.id == bw_set_card.rarity.id)), None)
        if not rarity_check and bw_set_card.rarity:
            final_bigweb_rarities.append(bw_set_card.rarity)

    for bw_set_card in final_bigweb_set_cards:
        condition_check = next(
            (bw_condition for bw_condition in final_bigweb_conditions if (bw_set_card.condition and bw_condition.id == bw_set_card.condition.id)), None)
        if not condition_check and bw_set_card.condition:
            final_bigweb_conditions.append(bw_set_card.condition)

    df_sets = pd.DataFrame([bigweb_set.get_dict()
                           for bigweb_set in final_bigweb_sets])

    df_set_cards = pd.DataFrame([set_card.get_dict(
    ) for set_card in final_bigweb_set_cards if set_card.condition and set_card.condition.id == 216])

    print("df_set_cards", df_set_cards)
    final_bigweb_set_cards_filtered = [
        set_card for set_card in final_bigweb_set_cards if set_card.condition and set_card.condition.id == 216 and (set_card.yugipedia_set_card_code and not check_for_jap_chars(set_card.yugipedia_set_card_code))]

    df__bigweb_latest_set_cards = pd.DataFrame(
        [set_card.get_yugioh_data_dict() for set_card in final_bigweb_set_cards_filtered])

    df_rarities = pd.DataFrame([rarity.get_dict()
                               for rarity in final_bigweb_rarities])
    df_conditions = pd.DataFrame([condition.get_dict()
                                 for condition in final_bigweb_conditions])

    df_sets.to_csv("bigweb_sets.csv", index=False)
    df_set_cards.to_csv("bigweb_set_cards.csv", index=False)
    df_rarities.to_csv("bigweb_rarities.csv", index=False)
    df_conditions.to_csv("bigweb_conditions.csv", index=False)
    # replace rarity for duel terminal
    df__bigweb_latest_set_cards = replace_rarity_main_for_duel_terminal(
        df=df__bigweb_latest_set_cards, set_card_code='card_code', rarity_column='mapped_rarity')

    df__bigweb_latest_set_cards.to_csv("bigweb_latest.test.csv", index=False)

    upload_data(df=df_sets,
                table_name="bigweb_sets", if_exist="replace", db_name="yugioh_data")
    upload_data(df=df_set_cards,
                table_name="bigweb_set_cards", if_exist="replace", db_name="yugioh_data")
    upload_data(df=df_rarities,
                table_name="bigweb_rarities", if_exist="replace", db_name="yugioh_data")
    upload_data(df=df__bigweb_latest_set_cards,
                table_name="bigweb_latest", if_exist="replace", db_name="yugioh_data")
    upload_data(df=df__bigweb_latest_set_cards,
                table_name="bigweb", if_exist="append", db_name="yugioh_data")
    end = datetime.datetime.now()
    difference = end - start
    print(f"The time difference between the 2 time is: {difference}")


def replace_duel_terminal_rarity(x):
    mapping_dict = {
        "Rare": "Duel Terminal Rare Parallel Rare",
        "Common": "Duel Terminal Normal Parallel Rare",
        "Ultra Rare": 'Duel Terminal Ultra Parallel Rare',
        "Super Rare": 'Duel Terminal Super Parallel Rare',
        "Secret Rare": 'Duel Terminal Secret Parallel Rare',
        "Normal Rare": 'Duel Terminal Normal Rare Parallel Rare'
    }
    if x in mapping_dict:
        return mapping_dict[x]
    else:
        return x


def replace_rarity_main_for_duel_terminal(df: pd.DataFrame, set_card_code: str | None = None, rarity_column: str | None = None) -> pd.DataFrame:
    print(df.columns)
    if not set_card_code:
        set_card_code = 'card_code'
    if not rarity_column:
        rarity_column = 'yugipedia_rarity_prefix'

    mask = (df[set_card_code].str.startswith(
        'DT', na=False))
    df_duel_terminal = df[mask]
    df.loc[mask, rarity_column] = df_duel_terminal[rarity_column].apply(
        lambda x: replace_duel_terminal_rarity(x))
    return df


if __name__ == "__main__":
    bigweb_scrape()
