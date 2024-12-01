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
    with sfconn.cursor() as cursor:
        cursor.execute(query)
        data = cursor.fetch_pandas_all()
    return data


def create_sf_warehouse(sf_conn, sf_object_type, sf_object_name):
    result = sf_conn.cursor().execute(f"CREATE {sf_object_type} IF NOT EXISTS {sf_object_name}")
    print(f'***{result}')
    return True


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
    create_sf_warehouse(sfconn, "WAREHOUSE", "my_newer_warehouse_ep")
    sfconn.cursor().execute("USE WAREHOUSE my_newer_warehouse_ep")

    create_sf_warehouse(sfconn, "DATABASE", "my_newer_database_ep")
    sfconn.cursor().execute("USE DATABASE my_new_database_ep")
    create_sf_warehouse(sfconn, "SCHEMA", "my_newer_schema_ep")
    sfconn.cursor().execute("USE SCHEMA my_newer_schema_ep")

    sfconn.cursor().execute(
        "CREATE OR REPLACE TABLE "
        "newer_test_table(col1 integer, col2 string)")
    sfconn.cursor().execute(
        "INSERT INTO newer_test_table(col1, col2) VALUES " +
        "    (123, 'test string1'), " +
        "    (456, 'test string2')")
    sfconn.cursor().close()
