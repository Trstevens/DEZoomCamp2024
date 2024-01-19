#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


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
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}', pool_pre_ping=True)

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)





# #!/usr/bin/env python
# # coding: utf-8
# import argparse #standard python library that allows you to work with positional arguments
# import os
# import pandas as pd
# # Create connection to Postgres
# from sqlalchemy import create_engine
# from time import time


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
    
#     # download the csv 
#     os.system(f"wget {url} - O {csv_name}")

#     # Create the sql engine to connect to postgres
#     engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

#     # connecting the engine
#     engine.connect()

#     # grab only first 100 rows
#     df = pd.read_csv(csv_name, compression='gzip', nrows=100)

#     # defining a pandas df iterator
#     df_iter = pd.read_csv(csv_name, compression='gzip', iterator=True, chunksize=100000)

#     # See if the table exists and basically truncate it based off the headers of the df
#     df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

#     while True:
#         t_start = time()
        
#         df = next(df_iter)
        
#         # Convert columns from text to datetime
#         df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
#         df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
#         df.to_sql(name=table_name, con=engine, if_exists='append')
        
#         t_end = time()
        
#         print('inserted another chunk... took %.3f second' % (t_end - t_start))

# if __name__ == '__main__':

#     # create parser
#     parser = argparse.ArgumentParser(
#                         prog='ingest_data.py',
#                         description='Ingest CSV data to Postgres',
#                         epilog='Text at the bottom of help')

#     parser.add_argument('--user', help='username for postgres')
#     parser.add_argument('--password', help='password for postgres')  
#     parser.add_argument('--host', help='host for postgres')  
#     parser.add_argument('--port', help='port for postgres')  
#     parser.add_argument('--db', help='database name for postgres')  
#     parser.add_argument('--table_name', help='name of the table where we will write results to')  
#     parser.add_argument('--url', help='url of the csv file')  

#     parser.add_argument('-c', '--count')      # option that takes a value
#     parser.add_argument('-v', '--verbose',
#                         action='store_true')  # on/off flag

#     args = parser.parse_args()
#     main(args)