#!/usr/bin/env python
# coding: utf-8

import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
import gzip
import shutil

def main(params):
    user = params.user
    password = params.password
    host =params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'
    gz_name = 'output.csv.gz'

    os.system(f"wget {url} -O {gz_name}")

    # Giải nén file .gz
    with gzip.open(gz_name, 'rb') as f_in:
        with open(csv_name, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    #Download the csv     
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df = df.drop(columns=['airport_fee'])

    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True:
        t_start = time()

        df = next(df_iter)
        df = df.drop(columns=['airport_fee'])

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

        df.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print(f'{len(df)} rows inserted in {t_end - t_start} seconds')

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest data from CSV to PostgreSQL')

    parser.add_argument('--user', type=str, help='PostgreSQL username', required=True)
    parser.add_argument('--password', type=str, help='PostgreSQL password', required=True)  
    parser.add_argument('--host', type=str, help='PostgreSQL host', required=True)
    parser.add_argument('--port', type=str, help='PostgreSQL port', required=True)
    parser.add_argument('--db', type=str, help='PostgreSQL database name', required=True)
    parser.add_argument('--table_name', type=str, help='PostgreSQL table name', required=True)
    parser.add_argument('--url', type=str, help='CSV file path', required=True)

    args = parser.parse_args()  

    main(args)
