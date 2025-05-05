import datetime
import logging
import os
import sys
import time

import pandas as pd
import pymysql

import requests

# Used to check if data exist on before and after


def get_inventory(list_of_products: list):
    list_of_inventory = []
    for i in list_of_products:
        if i[1] is not None:
            obj = {}
            obj['English name'] = str(i[1]).split("|")[3] if str(
                i[1]).split("|")[3] is not None else None
            obj['Quantity'] = i[2]
            obj['CardCode'] = i[1].split("|")[1] if i[1].split("|")[
                1] is not None else None
            obj['Rarity'] = i[1].split("|")[2] if i[1].split("|")[
                2] is not None else None
            obj['CardSet'] = obj['CardCode'].split(
                "-")[0] if "-" in obj['CardCode'] else None
            obj['CardNumber'] = obj['CardCode'].split(
                "-")[1] if "-" in obj['CardCode'] else None
            obj['Price'] = i[3]
            obj['ProductID'] = i[0]
            obj['Passcode'] = str(i[1]).split("|")[0] if str(
                i[1]).split("|")[0] is not None else None
        list_of_inventory.append(obj.copy())
    return list_of_inventory


def delete_products_with_old_name(list_of_products):
    consumer_key = os.getenv('WC_CLIENT_ID')
    consumer_secret = os.getenv('WC_SECRET_KEY')
    wcapi = API(
        url="https://tekkx.com",
        consumer_key=consumer_key,
        consumer_secret=consumer_secret,
        version="wc/v3"
    )

    list_of_return_obj = []
    if len(list_of_return_obj) == 0:
        return list_of_return_obj
    for product_id in list_of_products:
        api_call_string = "products/{product_id}".format(product_id=product_id)
        r = wcapi.delete(api_call_string, params={"force": True}).json()
        time.sleep(1)
        list_of_return_obj.append(r)

    return list_of_return_obj


def retrieve_website_data_to_remove_data():
    # rds settings
    start = datetime.datetime.now()
    rds_host = "tekkx-scalable-t2.c2d1ozubkqr4.ap-southeast-1.rds.amazonaws.com"
    name = os.getenv('user')
    password = os.getenv('password')
    db_name = os.getenv('db_name')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        # print(name,password,db_name)
        conn = pymysql.connect(host=rds_host, user=name,
                               passwd=password, db=db_name, connect_timeout=5)
    except pymysql.MySQLError as e:
        logger.error(
            "ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit()

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    """
    This function fetches content from MySQL RDS instance
    """
    item_count = 0

    with conn.cursor() as cur:
        cur.execute(
            "SELECT product_id,sku,stock_quantity,max_price FROM wp_wc_product_meta_lookup")

        list_of_products = []
        # print(list_of_products)
        [list_of_products.append(row) for row in cur]
        # [print(i) for i in list_of_products]

        list_of_inventory = get_inventory(list_of_products)
        # print(list_of_inventory)
        df = pd.DataFrame(list_of_inventory)
        df['Quantity'] = df['Quantity'].astype('int64')
        cols = ["English name", "CardSet", "CardNumber", "CardCode", "Rarity", "Quantity", "Price", "ProductID",
                "Passcode"]

        df = df[cols]
        # df = df.drop_duplicates(subset=['English name', 'CardSet', 'CardNumber', 'CardCode', 'Rarity'])
        df.to_csv('test2.csv', index=False)
        bucket_name = "yugioh-storage"
        dir_for_backup = ""
        filename_for_backup = "YGOInventoryV2.csv"

        end = datetime.datetime.now()
        print(start.strftime("%Y-%m-%d %H:%M:%S"))
        print(end.strftime("%Y-%m-%d %H:%M:%S"))
        difference = end - start
        print(f"The time difference between the 2 time is: {difference}")
    conn.commit()

    # return "Added %d items from RDS MySQL table" % (item_count)
    return df


def find_data_that_have_name_change(df_database):
    df_all = df_database.merge(
        df_database.drop_duplicates(
            keep='last', subset=['CardSet', 'CardNumber', 'CardCode', 'Rarity']),
        on=['English name', 'CardSet', 'CardNumber', 'CardCode', 'Rarity'], how='outer', indicator=True)

    print(df_all)
    df_all.to_csv('test3.csv', index=False)
    df_all_only_in_updated_left = df_all[df_all['_merge'] == 'left_only']
    df_all_only_in_updated_right = df_all[df_all['_merge'] == 'right_only']

    print(df_all_only_in_updated_left)
    print(df_all_only_in_updated_right)
    df_all_only_in_updated_left.to_csv('test1_left.csv', index=False)
    list_of_product_ids = df_all_only_in_updated_left['ProductID_x'].to_list()

    return list_of_product_ids


# Check for duplicated data. and to remove.
def main4_2():
    df_database = retrieve_website_data_to_remove_data()
    print(df_database)
    # list_of_product_ids = find_data_that_have_name_change(df_database)

    # response_obj_list = delete_products_with_old_name(list_of_product_ids)
    # [print(i) for i in response_obj_list]

    df_database_filtered = df_database['Passcode']

    df2 = pd.read_csv("D:\YGOInventoryV2.csv")
    df_all = df_database.merge(
        df2.drop_duplicates(keep='last', subset=[
                            'CardSet', 'CardNumber', 'CardCode', 'Rarity']),
        on=['English name', 'CardSet', 'CardNumber', 'CardCode', 'Rarity'], how='outer', indicator=True)

    print(df_all)
    df_all.to_csv('test3.csv', index=False)
    df_all_only_in_updated_left = df_all[df_all['_merge'] == 'left_only']
    df_all_only_in_updated_right = df_all[df_all['_merge'] == 'right_only']

    print(df_all_only_in_updated_left)
    print(df_all_only_in_updated_right)
    df_all_only_in_updated_left.to_csv('test1_left.csv', index=False)
    df_all_only_in_updated_right.to_csv('test1_right.csv', index=False)

    # list_of_product_ids = df_all_only_in_updated_left['ProductID_x'].to_list()


def send_telegram_msg(text_msg):
    BOT_API_KEY = os.getenv('BOT_API_KEY')
    CHANNEL_NAME = os.getenv('BOT_CHANNEL_NAME')
    # print(BOT_API_KEY, CHANNEL_NAME)

    URL = "https://api.telegram.org/bot{BOT_API_KEY}/sendMessage?chat_id={CHANNEL_NAME}&text={TEXT_MSG}".format(
        BOT_API_KEY=BOT_API_KEY, CHANNEL_NAME=CHANNEL_NAME, TEXT_MSG=text_msg)

    # print(URL)
    requests.get(URL)


if __name__ == "__main__":
    ###################################
    text_msg = "New Updates:\n" \
               "Cards in the Following Sets have been uploaded and updated to the website.\n" \
               "EXP2\n" \
               "EXP3\n" \
               "EP13\n" \
               "SHSP\n" \
               "JOTL\n\n" \
               "Please visit tekkx.com for more.\n\n"
    send_telegram_msg(text_msg)
    ###################################
