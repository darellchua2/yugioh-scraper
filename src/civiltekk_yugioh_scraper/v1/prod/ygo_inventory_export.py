import pandas as pd
import datetime
from sqlalchemy import text
import logging
import requests
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
import csv
from typing import Tuple, cast, List, Dict

from ..models.ygo_models import TekkxProductData
from .tcgcorner_scraper import get_card_prices
from ..utilities.aws_utilities import retrieve_data_from_db_to_df, get_engine_for_tekkx_scalable_db, save_df_to_mysql
from ..utilities.misc_utilities import get_file_path, split
from ..config import HEADERS, TABLE_YUGIOH_OVERALL_CARD_CODE_LISTS, MEDIAWIKI_URL, TEKKX_SCALABLE_DB_NAME


def create_overall_card_code_list() -> pd.DataFrame:
    """
    Fetches the overall card code list from the Yugioh database and processes it for inventory export.

    Returns:
        pd.DataFrame: A processed DataFrame of card codes with selected columns and initialized quantities.
    """
    try:
        df = retrieve_data_from_db_to_df(
            TABLE_YUGIOH_OVERALL_CARD_CODE_LISTS, db_name="yugioh_data")
        df['quantity'] = None
        cols = ['region', 'set_card_name_combined', 'set_name',
                'set_card_code_updated', 'rarity_name', 'quantity']
        return df[cols]
    except Exception as e:
        logging.error(f"Error fetching and processing card code list: {e}")
        return pd.DataFrame()


def retrieve_website_data() -> pd.DataFrame:
    """
    Retrieves product data from the WordPress database and processes it.

    Returns:
        pd.DataFrame: A DataFrame containing merged and formatted product data.
    """
    start = datetime.datetime.now()
    try:
        _, engine = get_engine_for_tekkx_scalable_db(
            db_name=TEKKX_SCALABLE_DB_NAME)
        with engine.begin() as conn:
            df_posts = pd.read_sql_query(
                sql=text("""
                    SELECT id AS product_id, post_title, post_type, post_mime_type, post_name
                    FROM wp_posts WHERE post_type IN ('product')
                """), con=conn
            )
            df_product_meta = pd.read_sql_query(
                sql=text("""
                    SELECT product_id, stock_quantity AS quantity, max_price AS price
                    FROM wp_wc_product_meta_lookup
                """), con=conn
            )

        df_product_meta['quantity'] = df_product_meta['quantity'].fillna(0)
        split_cols = df_posts['post_title'].str.split(r'\s*\|\s*', expand=True)
        df_posts['set_card_code_updated'] = split_cols[0]
        df_posts['set_card_name_combined'] = split_cols[1]
        df_posts['rarity_name'] = split_cols[2]
        df_posts['set_name'] = split_cols[3]
        df_posts['region'] = split_cols[4]

        df = pd.merge(df_posts, df_product_meta, on='product_id', how='left')
        df = df.dropna(subset=['set_card_name_combined'])
        df['quantity'] = df['quantity'].fillna(0).astype('int64')

        cols = ["region", "set_card_name_combined", "set_name", "set_card_code_updated",
                "rarity_name", "quantity", "price", "post_name", "post_title"]
        df = df[cols]
        df['duplicated'] = df.duplicated(
            subset=['set_card_code_updated', 'set_name', 'rarity_name', 'post_title'], keep='last')

        logging.info(f"Data retrieval time: {datetime.datetime.now() - start}")
        return df
    except Exception as e:
        logging.error(f"Error retrieving and processing website data: {e}")
        return pd.DataFrame()


def retrieve_website_data_to_list_of_dict() -> List[TekkxProductData]:
    """
    Retrieves product data from the WordPress database and converts it into a list of dictionaries.

    Returns:
        List[TekkxProductData]: List of product data as dictionaries.
    """
    list_of_results: List[TekkxProductData] = []
    start = datetime.datetime.now()

    try:
        _, engine = get_engine_for_tekkx_scalable_db(
            db_name=TEKKX_SCALABLE_DB_NAME)
        with engine.begin() as conn:
            df_posts = pd.read_sql_query(
                sql=text("""
                    SELECT id AS product_id, post_title, post_type, post_mime_type, post_name
                    FROM wp_posts WHERE post_type IN ('product')
                """), con=conn
            )
            df_product_meta = pd.read_sql_query(
                sql=text("""
                    SELECT product_id, stock_quantity AS quantity, max_price AS price
                    FROM wp_wc_product_meta_lookup
                """), con=conn
            )

        df_product_meta['quantity'] = df_product_meta['quantity'].fillna(0)
        df_posts = df_posts[df_posts['post_title'].str.count(r'\|') == 4]
        split_cols = df_posts['post_title'].str.split(r'\s*\|\s*', expand=True)
        df_posts['set_card_code_updated'] = split_cols[0]
        df_posts['set_card_name_combined'] = split_cols[1]
        df_posts['rarity_name'] = split_cols[2]
        df_posts['set_name'] = split_cols[3]
        df_posts['region'] = split_cols[4]

        df = pd.merge(df_posts, df_product_meta, on='product_id', how='left')
        df = df.dropna(subset=['set_card_name_combined'])
        cols = ["region", "set_card_name_combined", "set_name", "set_card_code_updated",
                "rarity_name", "quantity", "price", "post_name", "post_title"]
        df = df[cols]
        df['quantity'] = df['quantity'].fillna(0).astype('int64')

        df['duplicated'] = df.duplicated(
            subset=['set_card_code_updated', 'set_name', 'rarity_name', 'post_title'], keep='last')
        logging.info(f"Data retrieval time: {datetime.datetime.now() - start}")

        return cast(List[TekkxProductData], df.to_dict(orient="records"))
    except Exception as e:
        logging.error(f"Error retrieving and processing website data: {e}")
        return list_of_results


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
            url=MEDIAWIKI_URL, headers=HEADERS, params=obj, timeout=60).json()
        res_json_query_obj: dict = res_json.get("query", {})

        if "redirects" in res_json_query_obj:
            for item in res_json_query_obj["redirects"]:
                redirect_dict[item["from"]] = item["to"]

        while "continue" in res_json:
            obj["rdcontinue"] = res_json["continue"].get("rdcontinue", "")
            res_json = requests.get(
                url=MEDIAWIKI_URL, headers=HEADERS, params=obj, timeout=60).json()
            res_json_query_obj = res_json.get("query", {})

            if "redirects" in res_json_query_obj:
                for item in res_json_query_obj["redirects"]:
                    redirect_dict[item["from"]] = item["to"]

        return redirect_dict
    except requests.exceptions.JSONDecodeError as e:
        logging.error(f"JSONDecodeError in check_for_redirect: {e}")
        return redirect_dict


def export_inventory_excel(is_check_existing_names: bool = True, is_to_save_to_mysql: bool = False) -> None:
    """
    Combines website and database inventory data, handles card name redirections, and exports to Excel.
    """
    try:
        ygo_inventory_export_path = get_file_path("YGOInventoryV2.xlsx")
        ygo_overall_card_list_export_path = get_file_path(
            "OverallCardCodeList-2.xlsx")

        try:
            _, engine = get_engine_for_tekkx_scalable_db(db_name="yugioh_data")
            df_asian_english = pd.read_sql(
                "SELECT * FROM ygo_inventory_data WHERE region = 'Asian-English'", engine)
        except Exception as e:
            logging.warning(f"No Asian-English records found: {e}")
            df_asian_english = pd.DataFrame()

        df_website = pd.DataFrame(retrieve_website_data_to_list_of_dict())
        card_name_list = df_website["set_card_name_combined"].tolist()
        if is_check_existing_names:
            dict_to_map = check_existing_card_names_to_update(card_name_list)

            if dict_to_map:
                df_website = pd.merge(
                    df_website,
                    pd.DataFrame([{"name": k, "new name": v}
                                  for k, v in dict_to_map.items()]),
                    how="left",
                    left_on="set_card_name_combined",
                    right_on="name"
                ).drop(columns=['name'])

        df_combined = pd.concat(
            [df_website, df_asian_english], ignore_index=True)
        df_overall = create_overall_card_code_list()

        if ygo_inventory_export_path:
            with pd.ExcelWriter(ygo_inventory_export_path, engine='xlsxwriter') as writer:
                df_combined.to_excel(writer, sheet_name="V2", index=False)
        if ygo_overall_card_list_export_path:
            with pd.ExcelWriter(ygo_overall_card_list_export_path, engine='xlsxwriter') as writer:
                df_overall.to_excel(writer, sheet_name="Sheet1", index=False)

        if is_to_save_to_mysql:
            save_df_to_mysql(
                df_combined, table_name="ygo_inventory_data", if_exists="replace")

    except Exception as e:
        logging.error(f"Error exporting inventory to Excel: {e}")


def export_inventory_excel_v2(is_check_existing_names: bool = True, is_to_save_to_mysql: bool = False):
    """
    Combines website and database inventory data, handles card name redirections, and exports to Excel.
    """
    try:
        df_website = pd.DataFrame()
        ygo_inventory_export_path = get_file_path("YGOInventoryV2.xlsx")
        ygo_overall_card_list_export_path = get_file_path(
            "OverallCardCodeList-2.xlsx")

        try:
            _, engine = get_engine_for_tekkx_scalable_db(db_name="yugioh_data")
            df_asian_english = pd.read_sql(
                "SELECT * FROM ygo_inventory_data WHERE region = 'Asian-English'", engine)
        except Exception as e:
            logging.warning(f"No Asian-English records found: {e}")
            df_asian_english = pd.DataFrame()

        df_website = pd.DataFrame(retrieve_website_data_to_list_of_dict())
        card_name_list = df_website["set_card_name_combined"].tolist()
        if is_check_existing_names:
            dict_to_map = check_existing_card_names_to_update(card_name_list)

            if dict_to_map:
                df_website = pd.merge(
                    df_website,
                    pd.DataFrame([{"name": k, "new name": v}
                                  for k, v in dict_to_map.items()]),
                    how="left",
                    left_on="set_card_name_combined",
                    right_on="name"
                ).drop(columns=['name'])

        df_combined = pd.concat(
            [df_website, df_asian_english], ignore_index=True)

        # Drop duplicates based on specified columns, keeping the last occurrence
        df_combined = df_combined.drop_duplicates(
            subset=["region", "set_card_name_combined", "set_name",
                    "set_card_code_updated", "rarity_name"],
            keep='last'
        )

        df_overall = create_overall_card_code_list()

        if ygo_inventory_export_path:
            with pd.ExcelWriter(ygo_inventory_export_path, engine='xlsxwriter') as writer:
                df_combined.to_excel(writer, sheet_name="V2", index=False)
        if ygo_overall_card_list_export_path:
            with pd.ExcelWriter(ygo_overall_card_list_export_path, engine='xlsxwriter') as writer:
                df_overall.to_excel(writer, sheet_name="Sheet1", index=False)
        if is_to_save_to_mysql:
            save_df_to_mysql(
                df_combined, table_name="ygo_inventory_data", if_exists="replace")
        return df_website, df_overall, df_combined, df_asian_english
    except Exception as e:
        logging.error(f"Error exporting inventory to Excel: {e}")


def update_ae_price(list_of_inventory: List[Dict], list_of_card_prices: List[Dict]) -> List[Dict[str, str | float | bool | None]]:
    """
    Updates inventory prices based on matched card price data.

    Returns:
        List[Dict]: Inventory list with updated price fields.
    """
    try:
        items_dict = {
            (item['set_card_name_combined'], item['set_name'], item['rarity_name']): item for item in list_of_card_prices
        }
        for item in list_of_inventory:
            key = (item['set_card_name_combined'], item['set_name'],
                   item.get('rarity', item.get('rarity_name')))
            item['price'] = items_dict.get(key, {}).get('price', 0)
        return list_of_inventory
    except Exception as e:
        logging.error(f"Error updating AE prices: {e}")
        return list_of_inventory


def combine_ae_price(filename: str = "YGOInventoryV2-AE.xlsx",
                     sheet_name: str = "Inventory",
                     output_card_prices: bool = False,
                     output_updated_inventory: bool = False) -> tuple[list[dict[str, str | float | bool | None]], List[Dict[str, str | float | bool | None]]] | None:
    """
    Combines AE card price data into existing inventory and exports both raw and updated lists to CSV.
    """
    try:
        filepath = get_file_path(filename=filename)
        df_inventory = pd.read_excel(filepath, sheet_name=sheet_name)

        list_of_inventory = df_inventory.to_dict("records")
        card_prices = get_card_prices()

        updated_inventory = update_ae_price(list_of_inventory, card_prices)

        if output_card_prices:
            with open('list_of_card_prices.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, card_prices[0].keys())
                writer.writeheader()
                writer.writerows(card_prices)
        if output_updated_inventory:
            with open('list_of_updated_inventory.csv', 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, updated_inventory[0].keys())
                writer.writeheader()
                writer.writerows(updated_inventory)

        return card_prices, updated_inventory

    except Exception as e:
        logging.error(f"Error combining AE prices: {e}")


if __name__ == "__main__":
    try:
        combine_ae_price()
        export_inventory_excel()
    except Exception as e:
        logging.error(f"Error in main execution: {e}")
