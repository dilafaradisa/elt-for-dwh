import logging
import luigi
import pandas as pd
import time
import sqlalchemy
from datetime import datetime
from pipeline.utils.db_connect import dwh_db_connection
from pipeline.utils.read_sql import read_sql_file
from pipeline.load import LoadData
import os
from dotenv import load_dotenv

load_dotenv()

DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT", default=os.getcwd())
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_TRANSFORM_QUERY = os.getenv("DIR_TRANSFORM_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

class TransformData(luigi.Task):
    '''
    Task ini akan melakukan transformasi table dari skema stg ke skema final 
    menggunakan query sql yang sudah dibuat sebelunya dengan metode upsert
    '''

    def requires(self):
        return LoadData()
    
    def run(self):
        logging.basicConfig(level = logging.INFO,
                            filename=f'{DIR_TEMP_LOG}/logs.log',
                            format='%(asctime)s - %(levelname)s - %(message)s')
        
        # reading sql query from file
        try:
            dim_customer_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/dim_customers.sql')
            dim_product_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/dim_products.sql')
            dim_seller_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/dim_sellers.sql')
            fact_delivery_process_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/fact_delivery_process.sql')
            fact_order_payment_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/fact_order_payment.sql')
            fact_order_review_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/fact_order_review.sql')
            fact_order_sales_query = read_sql_file(f'{DIR_TRANSFORM_QUERY}/fact_order_sales.sql')

            logging.info("SQL queries for transformation loaded successfully.")

        except Exception as e:
            logging.error(f"Failed to read SQL query files")
            raise Exception(f"Failed to read SQL query files: {e}")
            
        # connect db
        try:
            dwh_engine = dwh_db_connection()
            logging.info("Connected to DWH database successfully.")
        except Exception as e:
            logging.error(f"Failed to connect to DWH database: {e}")
            return
            
        start_time = time.time()
        logging.info('----------Start transforming data from staging to final schema----------')
        # transform and load data into final schema
        try: 
            queries = [
                dim_customer_query, 
                dim_product_query, 
                dim_seller_query, 
                fact_delivery_process_query, 
                fact_order_payment_query, 
                fact_order_review_query, 
                fact_order_sales_query
                ]
            for idx, query in enumerate(queries):
                with dwh_engine.connect() as connection:
                    connection.execute(sqlalchemy.text(query))
                    connection.commit()
                    logging.info(f'Query {idx+1} executed successfully...')

            logging.info('----------Data transformation from staging to final schema completed successfully----------')

            end_time = time.time()
            execution_time = end_time - start_time

            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['TransformData'],
                'status': 'Success',
                'execution_time': [execution_time]
            }

            summary = pd.DataFrame(summary_data)

            summary.to_csv(f'{DIR_TEMP_DATA}/transform_summary.csv', index=False)

        except Exception as e:
            logging.error(f"Failed transforming tables")

            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['TransformData'],
                'status': 'Failed',
                'execution_time': [0]
            }
            summary = pd.DataFrame(summary_data)
            summary.to_csv(f'{DIR_TEMP_DATA}/transform_summary.csv', index=False)

            raise Exception(f"Failed transforming tables: {e}")
    
    def output(self):
        return luigi.LocalTarget(f'{DIR_TEMP_DATA}/transform_summary.csv')