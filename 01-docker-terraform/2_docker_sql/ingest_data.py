import os
from time import time
import pandas as pd
from sqlalchemy import create_engine
import argparse
# import pyarrow.parquet as pq
# trips = pq.read_table('trips.parquet',iterator=True, chunksize=100000)
# trips = trips.to_pandas()

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

# URL = 'https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv'




# URL='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'

# python ingest_data.py \
#   --user=root \
#   --password=root \
#   --host=localhost \
#   --port=5432 \
#   --db=ny_taxi \
#   --table_name=yellow_taxi_trips \
#   --url=${URL}

#!/usr/bin/env python
# coding: utf-8

# import os
# import argparse

# from time import time

# import pandas as pd
# from sqlalchemy import create_engine


# def main(params):
#     user = params.user
#     password = params.password
#     host = params.host 
#     port = params.port 
#     db = params.db
#     table_name = params.table_name
#     url = params.url
    
#     # the backup files are gzipped, and it's important to keep the correct extension
#     # for pandas to be able to open the file
#     if url.endswith('.csv.gz'):
#         csv_name = 'output.csv.gz'
#     else:
#         csv_name = 'output.csv'

#     os.system(f"wget {url} -O {csv_name}")

#     engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

#     df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

#     df = next(df_iter)

#     df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
#     df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

#     df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

#     df.to_sql(name=table_name, con=engine, if_exists='append')


#     while True: 

#         try:
#             t_start = time()
            
#             df = next(df_iter)

#             df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
#             df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

#             df.to_sql(name=table_name, con=engine, if_exists='append')

#             t_end = time()

#             print('inserted another chunk, took %.3f second' % (t_end - t_start))

#         except StopIteration:
#             print("Finished ingesting data into the postgres database")
#             break

# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

#     parser.add_argument('--user', required=True, help='user name for postgres')
#     parser.add_argument('--password', required=True, help='password for postgres')
#     parser.add_argument('--host', required=True, help='host for postgres')
#     parser.add_argument('--port', required=True, help='port for postgres')
#     parser.add_argument('--db', required=True, help='database name for postgres')
#     parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
#     parser.add_argument('--url', required=True, help='url of the csv file')

#     args = parser.parse_args()

#     main(args)

