import datetime
import os
import logging
import pymysql
import sys
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text
from woocommerce import API

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
            obj['Price'] = float(i[3])
            obj['product_id'] = i[0]

            list_of_inventory.append(obj.copy())
    return list_of_inventory


def get_inventory_null(list_of_products):
    list_of_inventory = []
    for i in list_of_products:
        if i[1] is None:
            obj = {}
            obj['product_id'] = i[0]

            list_of_inventory.append(obj.copy())
    return list_of_inventory


def main2():
    load_dotenv()
    # retrieve_website_data_to_remove2()


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


def postmeta_cleanup():
    # logger, conn = get_conn_for_tekkx_scalable_db()
    logger, engine = get_engine_for_tekkx_scalable_db(db_name=DB_NAME)

    with engine.begin() as conn:
        df_posts = pd.read_sql_query(
            sql=text(
                "SELECT id AS product_id, post_title, guid, post_type, post_mime_type FROM wp_posts WHERE post_type IN ('attachment')"),
            con=conn)

        df_postmeta = pd.read_sql_query(
            text("SELECT post_id AS product_id,meta_key,meta_value FROM wp_postmeta WHERE meta_key IN ('as3cf_filesize_total') and post_id > 10000"),
            con=conn)
        df_posts.to_csv("posts.csv", index=False)
        df_postmeta.to_csv("postmetas.csv", index=False)
        df_postmeta['check'] = df_postmeta['product_id'].isin(
            df_posts['product_id'])
        print(df_postmeta)
        df_postmeta.to_csv("post_check.csv", index=False)
        df_postmeta_filter = df_postmeta[df_postmeta['check'] == False]
        df_postmeta_filter.to_csv("post_check_filter.csv", index=False)
        list_to_del = df_postmeta_filter['product_id'].to_list()
        string_list_to_del = (',').join([str(x) for x in list_to_del])
        print("LIST: {list}".format(list=string_list_to_del))
        delete_statement = "DELETE FROM wp_postmeta WHERE post_id IN ({list_to_del})".format(
            list_to_del=string_list_to_del)
        print(delete_statement)
        # cur.execute(delete_statement)

    conn.commit()


"""
FINALIZED delete_unattached_attachment
"""


def delete_unattached_attachment():
    logger, engine = get_engine_for_tekkx_scalable_db(db_name=DB_NAME)

    with engine.begin() as conn:
        df_posts = pd.read_sql_query(
            sql=text("SELECT * FROM wp_posts WHERE post_type IN ('attachment')"),
            con=conn)

        df_posts['count'] = df_posts.groupby(
            ['post_title'])['post_title'].transform('count')
        df_posts['to_delete'] = df_posts['count'] > 1

        df_posts_unattached = df_posts.loc[df_posts['post_parent']
                                           == 0 & df_posts['to_delete']]

        df_posts.to_csv("df_posts.csv", index=False)
        df_posts_unattached.to_csv("df_posts_unattached.csv", index=False)

        list_of_product_id_to_remove = df_posts_unattached["ID"].to_list()
        ids_to_remove = (",").join([str(i)
                                    for i in list_of_product_id_to_remove])
        df_s3 = pd.read_sql_query(
            sql=text("SELECT * FROM tekkx_scalable.wp_as3cf_items WHERE source_id in ({ids_to_remove})".format(
                ids_to_remove=ids_to_remove)),
            con=conn)

        df_s3.to_csv("df_s3.csv", index=False)

    conn.commit()


if __name__ == "__main__":
    start = datetime.datetime.now()
    postmeta_cleanup()
    # delete_unattached_attachment()

    end = datetime.datetime.now()
    difference = end - start
    print(f"The time difference between the 2 time is: {difference}")
