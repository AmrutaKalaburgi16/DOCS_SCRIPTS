import sys
import time
import cx_Oracle
import pandas as pd
import re
import csv
import os
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from util import send_email_notification


DB_USER = 'docspce'
DB_PASSWORD = 'Docs$2025'
DB_DSN = 'lllpdb.corp.intranet:1581/LLLP'








if len(sys.argv) < 2:
    print("Usage: python export_docs_temp_table.py  phase tablename")

    sys.exit(1)
    
phase = sys.argv[1]
table_name= sys.argv[2]if len(sys.argv) > 2 else None 





#date='2018-10-06'
def connect_to_oracle():
    try:
        connection = cx_Oracle.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            dsn=DB_DSN
        )
        print(f"Connected to Oracle Database: {os.path.abspath(__file__)}")
        return connection
    except cx_Oracle.DatabaseError as e:
        print(f"Database connection error for {os.path.abspath(__file__)}: {e}")
        return None

DESTINATION_BASE_DIR = f"/PET1/prcoper/amruta/docs/outputs"
if not os.path.exists(DESTINATION_BASE_DIR):
    os.makedirs(DESTINATION_BASE_DIR, exist_ok=True)

current_dir = os.path.dirname(os.path.abspath(__file__))

log_file_path = os.path.abspath(f"Extract_script_{datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S_%f')}.log")
print('\nlog file path:\n' + log_file_path)

start_time = datetime.datetime.now()
print(f'Script started at: {start_time}')
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s -> %(message)s', datefmt='%Y-%m-%dT%I:%M:%S')
logging.info(f'Script started at: {start_time}')





queries = [
    f"SELECT * FROM {table_name} where phase='{phase}'"
    ]




print("queries defined successfully")

def execute_query_and_write_to_csv(query, DESTINATION_BASE_DIR):
    connection = None
    row_count = 0
    csv_filename = f"table_data.csv"  # Default filename in case table name extraction fails

    try:
        connection = connect_to_oracle()
        curs = connection.cursor()
        curs.arraysize = 5000
        curs.prefetchrows = 5000

        # print(f"Processing for Billing App ID: {billing_app_id}, Company Owner ID: {company_owner_id}")
        # logging.info(f"Processing for Billing App ID: {billing_app_id}, Company Owner ID: {company_owner_id}")

        # logging.info(f'Executing query: {formatted_query}')
       
        csv_filename = f'{table_name}_phase_{phase}.csv'
       

        csv_file_path = os.path.join(DESTINATION_BASE_DIR, csv_filename)
        mode = 'w'  # if is_first_batch else 'a'
        curs.execute(query)
        print(f"Query executed successfully at {datetime.datetime.now()}")

        with open(csv_file_path, mode=mode, newline='', encoding='utf-8', buffering=1) as file:
            writer = csv.writer(file)
            if os.stat(csv_file_path).st_size == 0:
                writer.writerow([i[0] for i in curs.description])
          #  rows = curs.fetchmany()
          #  while rows:
            for row in curs:
                writer.writerow(row)
                # writer.writerows(rows)
                row_count += 1


        print(f"Extracted {row_count:,} rows to {csv_filename}")
        logging.info(f"Extracted {row_count:,} rows to {csv_filename}")

        return row_count  # Returns 13900

    except cx_Oracle.DatabaseError as e:
        error, = e.args
        logging.error(f'Oracle-Error-Message: {error.message}')
    except csv.Error as e:
        logging.error(f'CSV Error: {e}')
    except Exception as e:
        logging.error(f'An unexpected error occurred: {e}')
    finally:

        if connection:
            curs.close()
            connection.close()
            print("Database connection closed")

# batch_size = 100
total_extracted = 0
# is_first_batch = True

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = []

    for query in queries:
        futures.append(executor.submit(execute_query_and_write_to_csv, query, DESTINATION_BASE_DIR))

    for future in as_completed(futures):
        try:
            result = future.result()
            if result:
                total_extracted += result
            print(f"Query completed successfully-{result:,} rows extracted")
        except Exception as e:
            print(f"Query failed: {e}")
            logging.error(f"Query failed: {e}")

send_email_notification({
    'status': 'SUCCESS' if total_extracted > 0 else 'NO_DATA',
    'script_start_time': start_time,
    'script_end_time': datetime.datetime.now(),
    'total_extracted': total_extracted  # Use total_extracted, not row_count
}, __file__)


print(f"EXTRACTION SUMMARY:")
print(f"Total records extracted: {total_extracted:,}")
end_time = datetime.datetime.now()
print(f'Script ended at: {end_time}')
logging.info(f'Script ended at: {end_time}')
time_taken = end_time - start_time
print(f'Total time taken: {time_taken}')
logging.info(f'Total time taken: {time_taken}')

