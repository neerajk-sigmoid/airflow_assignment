import sys
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import base64


def get_connection():
    sf_password = base64.b64decode('U2lnbW9pZDEyMzQ=').decode()
    conn = snowflake.connector.connect(
                    user='SIGMOIDUSER',
                    password= sf_password,
                    account="ljhdyiq-me93109",
                    warehouse='COMPUTE_DATA',
                    database='database',
                    schema='users_data'
                    )
    return conn


def get_dataframe():
    conn = get_connection()
    curs = conn.cursor()
    curs.execute("select * from DATABASE.USERS_DATA.PRODUCTS_DATA")
    res = curs.fetchall()
    headers = list(map(lambda t: t[0], curs.description))
    df = pd.DataFrame(res)
    df.columns = headers

    return df


def filter_dataframe(filter_value):
    df = get_dataframe()
    df = df[df['PRODUCT_PHOTOS_QTY'] == filter_value]
    output_file_name = "product_photos_qty_equals_"+str(filter_value) + ".csv"
    df.to_csv("/Users/neeraj/Desktop/"+output_file_name,index=False,header=True)


if __name__ == "__main__":
    params = json.loads(sys.argv[1])
    filter_dataframe(params['filter_value'])