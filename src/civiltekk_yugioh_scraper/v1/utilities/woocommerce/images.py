import pandas as pd
import datetime
import os
import pymysql
import logging
import sys


def get_attachment_from_db():
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
        conn = pymysql.connect(host=rds_host, user=name, passwd=password, db=db_name, connect_timeout=5)
    except pymysql.MySQLError as e:
        logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
        logger.error(e)
        sys.exit()

    logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")

    """
    This function fetches content from MySQL RDS instance
    """
    item_count = 0

    with conn.cursor() as cur:
        cur.execute("SELECT product_id,sku,stock_quantity,max_price FROM wp_wc_product_meta_lookup")