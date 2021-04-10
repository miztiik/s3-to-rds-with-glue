#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from logging import log
import mysql.connector
import json
import logging
import random
import datetime
import string
import time
import os
import socket
from mysql.connector import errorcode


class GlobalArgs:
    """ Global statics """
    OWNER = "Mystique"
    ENVIRONMENT = "production"
    MODULE_NAME = "insert_records_to_mysql"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    LOG_FILE_NAME = "/var/log/miztiik-automation-mysqldb-ingestor.log"
    DB_ADDRESS = "127.0.0.1"
    DB_ADMIN_NAME = "mysqladmin"
    DB_ADMIN_PASS = "Som3thingSh0uldBe1nVault"
    DB_NAME = "miztiik_db"
    DB_COLLECTIONS_1 = "customers"
    DB_COLLECTIONS_2 = "loyalty"
    NO_OF_RECORDS_TO_INSERT = 10
    INSERT_DURATION = 123


def random_str_generator(size=40, chars=string.ascii_uppercase + string.digits):
    ''' Generate Random String for given string length '''
    return ''.join(random.choice(chars) for _ in range(size))


def getReferrer():
    x = random.randint(1, 1000)
    x = x*10
    y = x+50
    data = {}
    product = random.randint(1, 250)
    random_str_generator(16)
    data['cust_id'] = random.randint(1, 100000)
    data['referrer'] = random.choice(['amazon.com', 'facebook.com', 'twitter.com', 'github.com',
                                      'miztiik', 'myanimelist.net', 'valaxy', 'Akane', 'mystiqueAutomation', 'kon', 'wikileaks'])
    data['url'] = random.choice(['ela_neer_vitrine_nav', 'ela_neer_product_detail',
                                 'ela_neer_vitrine_nav', 'ela_neer_checkout', 'ela_neer_product_detail', 'ela_neer_cart'])
    data['device'] = random.choice(['app_mobile', 'app_tablet', 'browser'])
    data['gender'] = random.choice(['M', 'F'])
    data['kiosk_id'] = 0
    # time.sleep(.5)
    # data['ts'] = str(datetime.datetime.now())
    if data['url'] != 'ela_neer_vitrine_nav':
        data['kiosk_id'] = product
    return data


def insert_records():

    # GET LOCAL IP
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    # print(f"Connecting to SQL at IP Address: {ip_address}")
    # client = pymongo.MongoClient("mongodb://mongoDbAdmin:Som3thingSh0uldBe1nVault@10.10.0.52/miztiik_db")
    # mongo -u mongoDbAdmin -p Som3thingSh0uldBe1nVault 10.10.0.123
    # mongodb://mongoDbAdmin:Som3thingSh0uldBe1nVault@18.236.250.136/miztiik_db
    # client = pymongo.MongoClient(GlobalArgs.DB_ADDRESS)
    connection = f"mongodb://{GlobalArgs.DB_ADMIN_NAME}:{GlobalArgs.DB_ADMIN_PASS}@{ip_address}/admin"
    client = pymongo.MongoClient(connection)
    db = client[GlobalArgs.DB_NAME]
    print(connection)
    print(db)
    customers_coll = db[GlobalArgs.DB_COLLECTIONS_1]
    loyalty_coll = db[GlobalArgs.DB_COLLECTIONS_2]
    begin_time = datetime.datetime.now()
    new_time = begin_time
    i = 0
    while (new_time - begin_time).total_seconds() < GlobalArgs.INSERT_DURATION:
        cust_data = getReferrer()
        result = db[GlobalArgs.DB_COLLECTIONS_1].insert_one(cust_data)
        print(f"customer_record_id:{result.inserted_id}")
        insert_loyalty_points(cust_data["cust_id"])
        new_time = datetime.datetime.now()
        i += 1
    print(f'{{"no_of_records_inserted":{i}}}')
    client.close()
    # print the number of documents in a collection
    print(f'{{"total_coll_count":{customers_coll.estimated_document_count()}}}')
    logging.info(
        f'{{"total_coll_count":{customers_coll.estimated_document_count()}}}')
    print(f'{{"total_loyalty_coll_count":{loyalty_coll.estimated_document_count()}}}')
    logging.info(
        f'{{"total_loyalty_coll_count":{loyalty_coll.estimated_document_count()}}}')


def ingest_helper(DB_NAME):
    begin_time = datetime.datetime.now()
    new_time = begin_time
    i = 0
    while (new_time - begin_time).total_seconds() < GlobalArgs.INSERT_DURATION:
        mysql_insert_customers(DB_NAME)
        new_time = datetime.datetime.now()
        i += 1
        if i % 1000 == 0:
            print(f'{{"records_inserted":{i}}}')
    print(f'{{"tot_of_records_inserted":{i}}}')
    logging.info(f'{{"tot_of_records_inserted":{i}}}')


def insert_loyalty_points(cust_id):
    hostname = socket.gethostname()
    ip_address = socket.gethostbyname(hostname)
    connection = f"mongodb://{GlobalArgs.DB_ADMIN_NAME}:{GlobalArgs.DB_ADMIN_PASS}@{ip_address}/admin"
    client = pymongo.MongoClient(connection)
    db = client[GlobalArgs.DB_NAME]
    loyalty_coll = db[GlobalArgs.DB_COLLECTIONS_2]
    data = {}
    data["cust_id"] = cust_id
    data["pts"] = random.randint(1, 2500)
    result = db[GlobalArgs.DB_COLLECTIONS_2].insert_one(data)
    print(f"customer_loyalty_record_id:{result.inserted_id}")
    client.close()


def mysql_insert_customers(DB_NAME):
    try:
        # GET LOCAL IP
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        # print(f"Connecting to SQL at IP Address: {ip_address}")
        cnx = mysql.connector.connect(
            host=ip_address,
            user=GlobalArgs.DB_ADMIN_NAME,
            password=GlobalArgs.DB_ADMIN_PASS
        )
        cursor = cnx.cursor()
        cnx.database = DB_NAME
        query = "INSERT INTO customers(cust_id,referrer,url,device,kiosk_id,gender) " \
                "VALUES(%s,%s,%s,%s,%s,%s)"

        upsert_query = """INSERT INTO customers (cust_id,referrer,url,device,kiosk_id,gender)
                            VALUES(%s,%s,%s,%s,%s,%s)
                            ON DUPLICATE KEY UPDATE
                                referrer = VALUES(referrer),
                                url = VALUES(url),
                                device = VALUES(device),
                                kiosk_id = VALUES(kiosk_id),
                                gender = VALUES(gender)
                        """

        cust_data = getReferrer()
        args = (
            str(cust_data["cust_id"]),
            str(cust_data["referrer"]),
            str(cust_data["url"]),
            str(cust_data["device"]),
            str(cust_data["kiosk_id"]),
            str(cust_data["gender"])
        )

        cursor.execute(upsert_query, args)

        # if not cursor.rowcount:
        #     print("row count not found")
        #     logger.warning("row count not found")
        cnx.commit()
    except mysql.connector.Error as err:
        logger.error(f"Failed to insert record: {str(err)}")
        print(f"Failed to insert record: {str(err)}")
    finally:
        cursor.close()
        cnx.close()


def create_database(cursor, DB_NAME):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print(f"Failed creating database: {str(err)}")
        logger.error(f"Failed creating database: {str(err)}")
        exit(1)


def create_db_if_not_exists(DB_NAME):
    cnx = mysql.connector.connect(
        host=GlobalArgs.DB_ADDRESS,
        user=GlobalArgs.DB_ADMIN_NAME,
        password=GlobalArgs.DB_ADMIN_PASS
    )
    cursor = cnx.cursor()
    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor, DB_NAME)
            print(f"Database {DB_NAME} created successfully.")
            logger.info(f"Database {DB_NAME} created successfully.")
        else:
            print(err)
            logger.error(f"Failed to create database: {str(err)}")
            exit(1)
    cursor.close()
    cnx.close()


def create_tables(DB_NAME):
    cnx = mysql.connector.connect(
        host=GlobalArgs.DB_ADDRESS,
        user=GlobalArgs.DB_ADMIN_NAME,
        password=GlobalArgs.DB_ADMIN_PASS
    )
    cursor = cnx.cursor()

    print(f"connected_to:{cnx}")

    cnx.database = DB_NAME
    TABLES = {}
    TABLES["customers"] = (
        "CREATE TABLE IF NOT EXISTS `customers` ("
        "  `cust_id` varchar(32) NOT NULL,"
        "  `referrer` varchar(30) NOT NULL,"
        "  `url` varchar(30) NOT NULL,"
        "  `device` varchar(30) NOT NULL,"
        "  `kiosk_id` varchar(30) NOT NULL,"
        "  `gender` enum('M','F') NOT NULL,"
        "  PRIMARY KEY (`cust_id`)"
        ") ENGINE=InnoDB"
    )

    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print(f"Creating table {table_name}")
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print(f"'{table_name}' already exists.")
            else:
                print(err.msg)
                logger.error(f"Failed creating table: {str(err)}")
        else:
            print(f"Sucessfully created table: {table_name}")
            logger.info(f"Sucessfully created table: {table_name}")
    cursor.close()
    cnx.close()


def show_total_count(DB_NAME):
    try:
        hostname = socket.gethostname()
        ip_address = socket.gethostbyname(hostname)
        # print(f"Connecting to SQL at IP Address: {ip_address}")
        cnx = mysql.connector.connect(
            host=ip_address,
            user=GlobalArgs.DB_ADMIN_NAME,
            password=GlobalArgs.DB_ADMIN_PASS
        )
        cnx.database = DB_NAME
        cursor = cnx.cursor()
        cursor.execute("SELECT COUNT(*) FROM customers")
        total_records = cursor.fetchone()[0]
        print(f'{{"total_records_in_table": {total_records}}}')
        logger.info(f'{{"total_records_in_table": {total_records}}}')
    except mysql.connector.Error as err:
        print(f"Unable to get record count: {str(err)}")
        logger.error(f"Unable to get record count: {str(err)}")
        exit(1)


logger = logging.getLogger()
logger = logging.getLogger()
logging.basicConfig(
    filename=f"{GlobalArgs.LOG_FILE_NAME}",
    filemode='a',
    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
    level=GlobalArgs.LOG_LEVEL
)


# create_db_if_not_exists(GlobalArgs.DB_NAME)
# create_tables(GlobalArgs.DB_NAME)
ingest_helper(GlobalArgs.DB_NAME)
show_total_count(GlobalArgs.DB_NAME)
