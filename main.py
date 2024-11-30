import os

import snowflake.connector
from dotenv import load_dotenv


def connect_to_mystuff():
    sfuser = 'SFerikpohl'
    sfpwd = os.getenv("SF_PASSWORD")
    sfacct = 'cyb07297.us-east-1'
    sfdb = "MY_SAMPLE_DB"
    sfwarehouse = "COMPUTE_WH"
    sfschema = 'PUBLIC'
    snowflake_connection = snowflake.connector.connect(user=sfuser,
                                                       password=sfpwd,
                                                       account=sfacct,
                                                       warehouse=sfwarehouse,
                                                       database=sfdb,
                                                       schema=sfschema)
    return snowflake_connection


def request_data(sfconn, query):

    try:
        with sfconn.cursor() as cursor:
            cursor.execute(query)
            data = cursor.fetch_pandas_all()
    finally:
        sfconn.close()
    return data


if __name__ == '__main__':
    load_dotenv()
    sfconn = connect_to_mystuff()
    query = ("SELECT "
             "CURRENT_WAREHOUSE(), "
             "CURRENT_SCHEMA(), "
             "* "
             "FROM "
             "MY_SAMPLE_DB.PUBLIC.PEOPLE_100 "
             "WHERE FIRST_NAME like 'S%' and sex not in ('Female');")
    sales = request_data(sfconn, query)
    print(sales)
