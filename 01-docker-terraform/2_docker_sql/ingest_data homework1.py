import os
from time import time
import pandas as pd
from sqlalchemy import create_engine
import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    
    # Ingesting yellow_tripdata_2019 data
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2019-01.csv.gz'
    else:
        csv_name = 'yellow_tripdata_2019-01.csv'

    os.system(f"wget {url} -O {csv_name}")
    
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    # engine.connect()
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')
    
    # More pytonic code :)
    for item in df_iter:
        # next(df_iter)
        t_start = time()
        item.tpep_pickup_datetime = pd.to_datetime(item.tpep_pickup_datetime)
        item.tpep_dropoff_datetime = pd.to_datetime(item.tpep_dropoff_datetime)
        item.to_sql(name=table_name, con=engine, if_exists='append')    
        t_end = time()
        print('Inserted another chunk, took %.3f second' % (t_end-t_start))

    # Ingesting taxi_zone_lookup
    
    csv_name2 = 'taxi_zone_lookup.csv'

# Arguments:

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host',help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()
    main(args)