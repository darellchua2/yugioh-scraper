import pandas as pd
import datetime
from sqlalchemy import text
import logging
import requests
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import csv

from .tcgcorner_scraper import get_card_prices
from ..utilities.aws_utilities import retrieve_data_from_db_to_df, get_engine_for_tekkx_scalable_db, save_df_to_mysql
from ..utilities.misc_utilities import get_file_path, split
from ..config import HEADERS, TABLE_YUGIOH_OVERALL_CARD_CODE_LISTS, MEDIAWIKI_URL, TEKKX_SCALABLE_DB_NAME


def create_overall_card_code_list() -> pd.DataFrame:
    """
    Fetches the overall card code list from the database and processes it.

    Returns:
        pd.DataFrame: A DataFrame containing the processed card code list.
    """
    try:
        df_overall_card_code_list = retrieve_data_from_db_to_df(
            TABLE_YUGIOH_OVERALL_CARD_CODE_LISTS, db_name="yugioh_data"
        )
        df_overall_card_code_list['quantity'] = None

        cols = ['region', 'set_card_name_combined', 'set_name',
                'set_card_code_updated', 'rarity_name', 'quantity']
        df_overall_card_code_list = df_overall_card_code_list[cols]

        return df_overall_card_code_list
    except Exception as e:
        logging.error(f"Error fetching and processing card code list: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error


def retrieve_website_data() -> pd.DataFrame:
    """
    Fetches product data from the MySQL database and processes it.

    Returns:
        pd.DataFrame: A DataFrame containing the website product data.
    """
    start = datetime.datetime.now()

    try:
        logger, engine = get_engine_for_tekkx_scalable_db(
            db_name=TEKKX_SCALABLE_DB_NAME)

        with engine.begin() as conn:
            df_posts = pd.read_sql_query(
                sql=text(
                    "SELECT id AS product_id, post_title, post_type, post_mime_type, post_name "
                    "FROM wp_posts WHERE post_type IN ('product')"
                ),
                con=conn
            )
            df_product_meta = pd.read_sql_query(
                sql=text(
                    "SELECT product_id, stock_quantity AS quantity, max_price AS price "
                    "FROM wp_wc_product_meta_lookup"
                ),
                con=conn
            )

        df_product_meta['quantity'] = df_product_meta['quantity'].fillna(0)
        pattern_split = r" \| "
        df_posts[['set_card_code_updated', "set_card_name_combined", 'rarity_name', 'set_name']] = df_posts['post_title'].str.split(
            pattern_split, n=4, expand=True)

        df = pd.merge(df_posts, df_product_meta, how='left', left_on=[
                      'product_id'], right_on=['product_id'])
        df = df.dropna(subset=['set_card_name_combined'])
        df['quantity'] = df['quantity'].astype('int64')

        cols = ["set_card_name_combined", "set_name", "set_card_code_updated",
                "rarity_name", "quantity", "price", "post_name"]
        df = df[cols]
        df['duplicated'] = df.duplicated(
            subset=['set_card_code_updated', 'set_name', 'rarity_name'], keep='last')
        df['region'] = "Japanese"

        end = datetime.datetime.now()
        logging.info(f"Data retrieval time: {end - start}")

        return df
    except Exception as e:
        logging.error(f"Error retrieving and processing website data: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error


def check_existing_card_names_to_update(card_name_list: list[str]) -> dict:
    """
    Checks for existing card names and updates their mappings using concurrent threads.

    Args:
        card_name_list (list[str]): List of card names to check for updates.

    Returns:
        dict: A dictionary mapping old card names to new card names.
    """
    redirect_dict = {}
    try:
        list_of_split_card_name_list = list(split(card_name_list, 50))

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(check_for_redirect, split_list)
                       for split_list in list_of_split_card_name_list]
            for future in concurrent.futures.as_completed(futures):
                redirect_dict.update(future.result())

        return redirect_dict
    except Exception as e:
        logging.error(f"Error checking and updating card names: {e}")
        return {}


def check_for_redirect(list_of_card_names: list[str]) -> dict:
    """
    Checks for redirects for a list of card names using the Yugipedia API.

    Args:
        list_of_card_names (list[str]): List of card names to check for redirects.

    Returns:
        dict: A dictionary mapping old card names to redirected card names.
    """
    redirect_dict = {}
    card_list_string = "|".join(list_of_card_names)

    obj = {
        "action": "query",
        "format": "json",
        "prop": "redirects",
        "titles": card_list_string,
        "redirects": 1,
        "rdlimit": "500"
    }

    try:
        res_json = requests.get(
            url=MEDIAWIKI_URL, headers=HEADERS, params=obj).json()
        res_json_query_obj: dict = res_json.get("query", {})

        if "redirects" in res_json_query_obj:
            for item in res_json_query_obj["redirects"]:
                redirect_dict[item["from"]] = item["to"]

        while "continue" in res_json:
            obj["rdcontinue"] = res_json["continue"].get("rdcontinue", "")
            res_json = requests.get(
                url=MEDIAWIKI_URL, headers=HEADERS, params=obj).json()
            res_json_query_obj = res_json.get("query", {})

            if "redirects" in res_json_query_obj:
                for item in res_json_query_obj["redirects"]:
                    redirect_dict[item["from"]] = item["to"]

        return redirect_dict
    except requests.exceptions.JSONDecodeError as e:
        logging.error(f"JSONDecodeError in check_for_redirect: {e}")
        return redirect_dict


def export_inventory_excel():
    """
    Updates Japanese entries from WordPress, keeps Asian English entries from DB, and saves the combined data.
    """
    try:
        # Output paths
        ygo_inventory_filename = "YGOInventoryV2.xlsx"
        ygo_overall_card_list_filename = "OverallCardCodeList-2.xlsx"
        ygo_inventory_export_path = get_file_path(ygo_inventory_filename)
        ygo_overall_card_list_export_path = get_file_path(
            ygo_overall_card_list_filename)

        # 🟡 STEP 1: Pull latest Asian English entries from MySQL (if any)
        try:
            _, engine = get_engine_for_tekkx_scalable_db(db_name="yugioh_data")
            query = "SELECT * FROM ygo_inventory_data WHERE region = 'Asian-English'"
            df_asian_english = pd.read_sql_query(query, engine)
        except Exception as e:
            logging.warning(f"No Asian English records found in DB yet: {e}")
            df_asian_english = pd.DataFrame()

        # 🔵 STEP 2: Pull updated Japanese entries from WordPress
        df_website = retrieve_website_data()

        # 🔁 STEP 3: Handle any name redirections
        card_name_list = df_website["set_card_name_combined"].to_list()
        dict_to_map = check_existing_card_names_to_update(card_name_list)
        if dict_to_map:
            df_to_replace_names = pd.DataFrame(
                [{"name": key, "new name": value}
                    for key, value in dict_to_map.items()]
            )
            df_website = pd.merge(
                df_website, df_to_replace_names, how="left",
                left_on="set_card_name_combined", right_on="name"
            ).drop(columns=['name'])

        # 🧩 STEP 4: Combine Japanese + Asian English
        df_combined = pd.concat(
            [df_website, df_asian_english], ignore_index=True)

        # 🧾 STEP 5: Rebuild overall card code list (if needed)
        df_overall_card_code_list_mapped = create_overall_card_code_list()

        # 💾 STEP 6: Export combined to Excel
        if ygo_inventory_export_path:
            with pd.ExcelWriter(ygo_inventory_export_path, engine='xlsxwriter') as writer:
                df_combined.to_excel(writer, sheet_name="V2", index=False)
        if ygo_overall_card_list_export_path:
            with pd.ExcelWriter(ygo_overall_card_list_export_path, engine='xlsxwriter') as writer:
                df_overall_card_code_list_mapped.to_excel(
                    writer, sheet_name="Sheet1", index=False)

        # 🛢 STEP 7: Upload back to DB as the new source of truth
        save_df_to_mysql(
            df_combined, table_name="ygo_inventory_data", if_exists="replace")

    except Exception as e:
        logging.error(f"Error exporting inventory to Excel: {e}")


def update_ae_price(list_of_inventory: list[dict], list_of_card_prices: list[dict]) -> list[dict]:
    """
    Updates inventory prices based on card prices.

    Args:
        list_of_inventory (list[dict]): List of inventory dictionaries.
        list_of_card_prices (list[dict]): List of card price dictionaries.

    Returns:
        list[dict]: Updated list of inventory with prices.
    """
    try:
        items_dict_2 = {
            (card_price['set_card_name_combined'], card_price['set_name'], card_price['rarity_name']): card_price
            for card_price in list_of_card_prices
        }

        for inventory in list_of_inventory:
            key = (inventory['set_card_name_combined'],
                   inventory['set_name'], inventory['rarity'])
            inventory['price'] = items_dict_2.get(key, {}).get('price', 0)

        return list_of_inventory
    except Exception as e:
        logging.error(f"Error updating AE prices: {e}")
        return list_of_inventory  # Return the original list if an error occurs


def combine_ae_price():
    """
    Combines the AE price data and exports it to a CSV file.
    """
    try:
        inventory_ae_filename = "YGOInventoryV2-AE.xlsx"
        filepath = get_file_path(inventory_ae_filename)
        sheet_name = "Inventory"
        df_inventory: pd.DataFrame = pd.read_excel(
            filepath, sheet_name=sheet_name)

        list_of_inventory = df_inventory.to_dict("records")
        card_prices = get_card_prices()

        list_of_updated_inventory = update_ae_price(
            list_of_inventory=list_of_inventory, list_of_card_prices=card_prices)

        with open('list_of_card_prices.csv', 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(output_file, card_prices[0].keys())
            dict_writer.writeheader()
            dict_writer.writerows(card_prices)

        with open('list_of_updated_inventory.csv', 'w', newline='', encoding='utf-8') as output_file:
            dict_writer = csv.DictWriter(
                output_file, list_of_updated_inventory[0].keys())
            dict_writer.writeheader()
            dict_writer.writerows(list_of_updated_inventory)

    except Exception as e:
        logging.error(f"Error combining AE prices: {e}")


if __name__ == "__main__":
    try:
        combine_ae_price()
        export_inventory_excel()
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
