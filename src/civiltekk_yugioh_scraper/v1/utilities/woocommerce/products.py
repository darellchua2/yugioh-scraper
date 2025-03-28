import pandas as pd
import datetime
import os
import pymysql
import logging
import sys
from woocommerce import API
import numpy as np
from dotenv import load_dotenv
import html
import io
from sqlalchemy import create_engine, text


from yugioh_scraper.utilities.aws_utilities import get_engine_for_tekkx_scalable_db

"""   LOAD ENV VARIABLES START   """
load_dotenv()
RDS_HOST = os.getenv('RDS_HOST')
NAME = os.getenv('user')
PASSWORD = os.getenv('password')
DB_NAME = os.getenv('db_name')
WC_CLIENT_ID = os.getenv("WC_CLIENT_ID")
WC_SECRET_KEY = os.getenv("WC_SECRET_KEY")

wcapi = API(
    url="https://tekkx.com",  # Your store URL
    consumer_key=WC_CLIENT_ID,  # Your consumer key
    consumer_secret=WC_SECRET_KEY,  # Your consumer secret
    wp_api=True,  # Enable the WP REST API integration
    version="wc/v3",  # WooCommerce WP REST API version
    timeout=120,
    query_string_auth=True
)
"""   LOAD ENV VARIABLES END   """


def get_s3_csv() -> pd.DataFrame:
    df = pd.read_csv(
        "https://yugioh-storage-public.s3.ap-southeast-1.amazonaws.com/ygo_inventory_final.csv")
    return df


def retrieve_data_from_db_to_df(table_name) -> pd.DataFrame:
    # rds settings
    start = datetime.datetime.now()
    item_count = 0

    # logger, conn = get_conn_for_tekkx_scalable_db()
    logger, engine = get_engine_for_tekkx_scalable_db(db_name=DB_NAME)

    with engine.begin() as conn:
        df = pd.read_sql_query(
            sql=text("SELECT * FROM {table_name}".format(table_name=table_name)), con=conn)
    return df


def get_inventory3(list_of_products):
    list_of_inventory = []
    for i in list_of_products:
        print(i)
        if i[1] is not None and i[1] != "":
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
            obj['Card Set Name'] = i[1].split("|")[4] if i[1].split("|")[
                4] is not None else None
            # obj['CardNumber'] = obj['CardCode'].split("-")[1] if "-" in obj['CardCode'] else None
            obj['Price'] = float(i[3])
            obj['product_id'] = i[0]

            list_of_inventory.append(obj.copy())
    return list_of_inventory


def get_conn_for_tekkx_scalable_db():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        # print(name,password,db_name)
        conn = pymysql.connect(host=RDS_HOST, user=NAME,
                               passwd=PASSWORD, db=DB_NAME, connect_timeout=5)
    except pymysql.MySQLError as e:
        logger.error(
            "ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit()

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    return logger, conn


def update_product_price():
    # logger, conn = get_conn_for_tekkx_scalable_db()
    logger, engine = get_engine_for_tekkx_scalable_db(db_name=DB_NAME)

    with engine.begin() as conn:
        df = pd.read_sql_query(
            sql=text(
                "SELECT post_id AS product_id,meta_key,meta_value FROM wp_postmeta WHERE meta_key IN ('_sku','_regular_price')"),
            con=conn)

        ### UPDATE DF ###
        df = get_df_postmeta(df)
        df_s3_inventory = get_s3_csv()

        df_price_df = get_df_price_check(df, df_s3_inventory)
        df_price_df = df_price_df.dropna(subset=['SG Price'])
        product_list_to_update_price = df_price_df.to_dict('records')
        print("The records to be updated are: {size}".format(
            size=len(product_list_to_update_price)))
        [print(x['sku'], x['SG Price'], x['regular_price'])
         for x in product_list_to_update_price]
        #### update price in wcapi ####
        for i in range(0, len(product_list_to_update_price), 15):
            product_ids_in_10 = product_list_to_update_price[i:i + 15]
            if len(product_ids_in_10) > 0:
                data = {
                    "update": [
                        {
                            'id': product['product_id'],
                            'regular_price': str(product['SG Price'])
                        }
                        for product in product_ids_in_10
                    ]
                }
                print(data)
                try:
                    res = wcapi.post("products/batch", data).json()
                    print(res)
                except TimeoutError as e:
                    logger = logging.getLogger()
                    logger.setLevel(logging.INFO)


def get_df_postmeta(df_postmeta):
    df_price = df_postmeta[df_postmeta['meta_key'] == '_regular_price'].rename(
        columns={'meta_value': 'regular_price'}).drop(
        columns=['meta_key']).astype({'regular_price': 'float'})
    df_sku = df_postmeta[df_postmeta['meta_key'] == '_sku'].rename(columns={'meta_value': 'sku'}).drop(
        columns=['meta_key'])
    df_sku[['passcode', 'cardcode', 'rarity', 'cardname', 'cardset']
           ] = df_sku['sku'].str.split('|', n=5, expand=True)
    df_sku.to_csv("sku.csv", index=False)
    df_price.to_csv("regular_price.csv", index=False)
    df_postmeta = pd.merge(df_sku, df_price,
                           how='inner',
                           left_on=['product_id'],
                           right_on=['product_id']
                           )
    # df.to_csv('product.csv', index=False)
    return df_postmeta


def get_df_price_check(df, df_s3_inventory):
    df_merge = pd.merge(df, df_s3_inventory,
                        how='left',
                        left_on=['cardname', 'cardset', 'rarity'],
                        right_on=['English name', 'Card Set Name', 'Rarity'])
    df_merge['prices_match'] = np.where(
        df_merge['regular_price'] == df_merge['SG Price'], True, False)
    # df_merge.to_csv("merge_base.csv", index=False)
    df_price_dif = df_merge[df_merge['prices_match'] == False]
    return df_price_dif


# def get_product_from_wp_posts():

def get_product_categories():
    try:
        list_of_product_categories = []
        obj = {}
        count = 1

        res = wcapi.get(
            "products/categories?per_page=100&page={count}".format(count=count)).json()
        obj.update({cat['name']: cat['id'] for cat in res})
        print("count: {count}".format(count=count))
        while len(res) > 0:
            count += 1
            print("count: {count}".format(count=count))
            list_of_product_categories.extend(res.copy())
            res = wcapi.get(
                "products/categories?per_page=100&page={count}".format(count=count)).json()
            obj.update({cat['name']: cat['id'] for cat in res})

        df = pd.DataFrame(list_of_product_categories)
        df['name'] = df['name'].apply(lambda x: html.unescape(x).strip())
        df.to_csv("woocommerce\product_categories.csv", index=False)
        # print(obj)
        return obj

    except TimeoutError as e:
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        return {}


def create_product():
    df_s3 = pd.read_csv(
        "woocommerce\ygo_inventory_final (9).csv", dtype={'id': 'object'})
    product_category_dict = get_product_categories()

    """ CHECK PRODUCT CATEGORIES  START """
    list_of_categories_not_in_product_category_dict = []

    unique_cardset_from_s3 = df_s3['Card Set Name'].apply(
        lambda x: html.unescape(x).strip()).unique().tolist()
    print("unique_cardset", unique_cardset_from_s3)
    print("dict", product_category_dict.keys())
    product_category_list = [html.unescape(
        x).strip() for x in product_category_dict.keys()]

    [list_of_categories_not_in_product_category_dict.append(cardset)
     if cardset not in product_category_list else None
     for cardset in unique_cardset_from_s3]

    [print(x) for x in list_of_categories_not_in_product_category_dict]

    if len(list_of_categories_not_in_product_category_dict) > 0:
        category_data = {
            "create": [
                {
                    "name": category
                }
                for category in list_of_categories_not_in_product_category_dict
            ]
        }
        print(category_data)
        try:
            res = wcapi.post("products/categories/batch", category_data).json()
            print(res)
            product_category_dict = get_product_categories()
        except:
            logger = logging.getLogger()
            logger.setLevel(logging.INFO)
    """ CHECK PRODUCT CATEGORIES END """

    # logger, conn = get_conn_for_tekkx_scalable_db()
    logger, engine = get_engine_for_tekkx_scalable_db(db_name=DB_NAME)

    with engine.begin() as conn:
        df_posts = pd.read_sql_query(
            sql=text("SELECT id AS product_id, post_title, guid, post_type, post_mime_type FROM wp_posts WHERE post_type IN ('product','attachment')"),
            con=conn)

        df_posts.to_csv("woocommerce\product_wp_posts.csv", index=False)
        df_posts_product = df_posts[(df_posts['post_type'] == 'product')].astype({
            'product_id': 'object'})

        # pattern = r"(.*) \| (.+) \| (.+) \| (.+)"
        # df_posts_product['post_title_match'] = df_posts_product['post_title'].str.match(pattern)
        pattern_split = r" \| "
        df_posts_product[['cardcode', 'cardname', 'rarity', 'cardset']] = df_posts_product['post_title'].str.split(
            pattern_split, n=4,
            expand=True)
        df_posts_product.to_csv("woocommerce\product.csv", index=False)

        df_merge = pd.merge(df_s3, df_posts_product,
                            how="left",
                            left_on=['English name',
                                     'Rarity', 'Card Set Name'],
                            right_on=['cardname', 'rarity', 'cardset'],
                            indicator=True
                            )
        df_merge.to_csv("woocommerce\merge.csv", index=False)

        df_merge = df_merge[(df_merge['product_id'].isna())]
        list_of_product_to_create = df_merge.to_dict('records')

        #### to be commented out ####
        if len(list_of_product_to_create) > 0:
            list_of_product_to_create = [list_of_product_to_create[0]]
        #### to be commented out ####

        for i in range(0, len(list_of_product_to_create), 15):
            product_ids_in_10 = list_of_product_to_create[i:i + 15]
            # [print('x'['English name'],x['Card Set Name']) for x in product_ids_in_10]
            if len(product_ids_in_10) > 0:
                data = {
                    "create": [
                        {
                            'name': "{cardcode} | {cardname} | {rarity} | {cardset}"
                            .format(cardcode=product['CardCode'],
                                    cardname=product['English name'],
                                    rarity=product['Rarity'],
                                    cardset=product['Card Set Name']
                                    ),
                            'regular_price': str(product['SG Price']),
                            'type': 'simple',
                            "sku": "{passcode}|{cardcode}|{rarity}|{cardname}|{cardset}"
                            .format(passcode=product['id'],
                                    cardcode=product['CardCode'],
                                    rarity=product['Rarity'],
                                    cardname=product['English name'],
                                    cardset=product['Card Set Name']
                                    ),
                            "stock_quantity": product['Quantity'],
                            "stock_status": "instock" if product['Quantity'] > 0 else "outofstock",
                            "backorders": "no",
                            "description": "Brand: Konami\n\nCard Name: {cardname}\nCode: {cardcode}\nRarity: {rarity}\nPasscode: {passcode}\nType: {type}\nRace: {race}\nArchetype: {archetype}\n\nLEVEL: {level}\nATK: {atk}\nDEF: {DEF}\n\nDescription:\n{description}".format(
                                passcode=product['id'],
                                cardcode=product['CardCode'],
                                rarity=product['Rarity'],
                                cardname=product['English name'],
                                cardset=product['Card Set Name'],
                                type=product['type'],
                                archetype=product['archetype'],
                                level=product['level'],
                                atk=product['atk'],
                                DEF=product['def'],
                                description=product['desc'],
                                race=product['race']
                            ),
                            "short_description": product['desc'],
                            "manage_stock": True,
                            # "images": [product['image_url']],
                            "meta_data": [
                                {
                                    'key': '_yoast_wpseo_metadesc',
                                    'value': "Stock: {quantity} in stock | {desc}".format(
                                        quantity=product['Quantity'],
                                        desc=product['desc']
                                    )
                                },
                                {
                                    'key': "_length",
                                    'value': "0.086"
                                },
                                {
                                    'key': "_width",
                                    'value': "0.059"
                                },
                                {
                                    'key': "_height",
                                    'value': "0.001"
                                },
                                {
                                    'key': "_weight",
                                    'value': "0.002"
                                },
                                {
                                    'key': "_yoast_wpseo_title",
                                    'value': "{cardcode} | {cardname} | {rarity}".format(
                                        cardcode=product['CardCode'],
                                        rarity=product['Rarity'],
                                        cardname=product['English name']
                                    )
                                }
                            ],
                            "categories": [
                                {
                                    "id": product_category_dict[product['Card Set Name']]
                                }
                            ]
                        }
                        for product in product_ids_in_10
                    ]
                }
                # [print(data['create'][x]['name']) for x in range(0, len(product_ids_in_10))]
                [print(data['create'][x])
                 for x in range(0, len(product_ids_in_10))]

                try:
                    res = wcapi.post("products/batch", data).json()
                    print(res)
                except TimeoutError as e:
                    print("e:{error}".format(error=e))
                    logger = logging.getLogger()
                    logger.setLevel(logging.INFO)


def update_pxmi_imports_unique_key():
    logger, conn = get_conn_for_tekkx_scalable_db()
    logger, engine = get_engine_for_tekkx_scalable_db(db_name=DB_NAME)

    with engine.begin() as conn:
        df_pxmi_posts = pd.read_sql_query(
            sql=text("SELECT * FROM tekkx_scalable.wp_pmxi_posts"), con=conn)

        df_pxmi_posts.to_csv("pxmi_posts.csv", index=False)

        df_posts_product = pd.read_sql_query(
            sql=text("SELECT id AS product_id, post_title, guid, post_type, post_mime_type FROM wp_posts WHERE post_type IN ('product')"), con=conn)

        pattern_split = r" \| "
        df_posts_product[['cardcode', 'cardname', 'rarity', 'cardset']] = df_posts_product['post_title'].str.split(
            pattern_split, n=4,
            expand=True)

        df_merge = pd.merge(df_pxmi_posts, df_posts_product, left_on=[
                            'post_id'], right_on=['product_id'], how='left')

        df_merge['unique_key_updated'] = df_merge['cardname'] + \
            " | " + df_merge['cardset'] + " | " + df_merge['rarity']

        df_merge.to_csv("merge.csv", index=False)
        list_of_records = df_merge.to_dict(orient='records')

        list_of_records_string = [
            "({id},{product_id},{import_id},{unique_key},{product_key},{iteration},{specified})"
            .format(id=x['id'],
                    product_id=x['post_id'],
                    import_id=str(x['import_id']).replace(".0", ""),
                    unique_key="\'" +
                    str(x['unique_key_updated']).replace("'", "''") + "\'",
                    product_key='\'\'',
                    iteration=x['iteration'],
                    specified=x['specified'])
            for x in list_of_records]

        records_concat_2 = ",".join([str(x) for x in list_of_records_string])

        print(records_concat_2)
        update_string_2 = "INSERT INTO `wp_pmxi_posts` VALUES {records_concat_2}".format(
            records_concat_2=records_concat_2)
        print(update_string_2)
        # res = cur.execute(update_string_2)
        # print(res)

    conn.commit()


# update_product_price()
# create_product()
# get_product_categories()
# update_pxmi_imports_unique_key()

logger, conn = get_conn_for_tekkx_scalable_db()
with conn.cursor() as cur:
    df_merge = pd.read_csv("merge_base.csv")
    print(df_merge)
    # df_merge['update_string'] = df_merge['post_id'] + "," + (df_merge['import_id'] if df_merge['import_id'] != np.nan) + "," +df_merge['unique_key_updated'] + ",\'\'," +df_merge['iteration'] + "," +df_merge['specified']
    # df_merge.to_csv("modded.csv",index=False)
    list_of_records = df_merge.to_dict(orient='records')
    list_of_records_string = [
        "({id},{product_id},{import_id},{unique_key},{product_key},{iteration},{specified})"
        .format(id=x['id'],
                product_id=x['post_id'],
                import_id=str(x['import_id']).replace(".0", ""),
                unique_key="\'" +
                str(x['unique_key_updated']).replace("'", "''") + "\'",
                product_key='\'\'',
                iteration=x['iteration'],
                specified=x['specified'])
        for x in list_of_records]

    records_concat_2 = ",".join([str(x) for x in list_of_records_string])
    records_concat_2 = records_concat_2 + ";"
    text_file = "data.txt"
    with io.open(text_file, "w", encoding="utf-8") as f:
        for record in list_of_records_string:
            update_string_2 = "INSERT INTO `wp_pmxi_posts` VALUES {records_concat_2};".format(
                records_concat_2=record)
            print(update_string_2)
            f.write(update_string_2 + "\n")
            # res = cur.execute(update_string_2)
