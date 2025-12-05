import datetime
import subprocess
import cx_Oracle
import csv
import os

# DB_USER = "docspce"
# DB_PASSWORD = "Docs$2024"
# DB_DSN = "lllpdb.corp.intranet:1581/LLLP"

DB_USER = "devapp10c"
DB_PASSWORD = "devapp10c"
DB_DSN = "racorad15-scan.corp.intranet:1521/ensdv01d_s"

current_dir = os.path.dirname(os.path.abspath(__file__))
OUTPUT_CSV_FILE = os.path.join(current_dir, "outputs", "Document_Final_Phase3_" + datetime.datetime.now().strftime("%Y%m%d%H%M%S") + ".csv")

SQL_QUERY = """
  select TASK_ID,DOCUMENT_SUB_PATH,DOCUMENT_FILE_NAME,ORIGINAL_FILE_NAME,USER_CREATE_DATE,USER_MODIFY_DATE,FILE_TRANSFER_DATE,FILE_STATUS,GCP_PATH
  ,GCP_FILENAME from documents_3
--FETCH FIRST 1000 ROWS ONLY
"""

def send_email_notification(obj):
    Subject=f"STATUS: {os.path.basename(__file__)}"

    Body = f"""
    STATUS: {obj['status']}
    Total records exported: {obj['total_fetched']}
    Started: {obj['script_start_time']}
    Ended: {obj['script_end_time']}
    Time taken: {obj['script_end_time']-obj['script_start_time']}
    File location: {obj['file_loc']}
    -----------
    """
    body_str_encoded_to_byte = Body.encode()
    subprocess.run(
        ["mail", "-s", Subject,  "Amruta.Kalaburgi@lumen.com"],
        input=body_str_encoded_to_byte,
    )
    print("Email sent!")

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

def export_to_csv():
    connection = connect_to_oracle()
    if not connection:
        return

    status = "OK"
    total_exported = 0  # Add this line
    try:
        fetch_cursor = connection.cursor()
        fetch_cursor.arraysize = 5000  # Fetch 1000 rows at a time
        script_start_time = datetime.datetime.now()
        print(f"Starting  data export to file {os.path.basename(__file__)} at {script_start_time}")

        fetch_cursor.execute(SQL_QUERY)
        print(f"Query executed successfully at {datetime.datetime.now()}")

        with open(OUTPUT_CSV_FILE, mode="w", newline="", encoding="utf-8") as csv_file:
            writer = csv.writer(csv_file)

            column_names = [col[0] for col in fetch_cursor.description]
            writer.writerow(column_names)

            for row in fetch_cursor:
                writer.writerow(row)
                total_exported += 1  # Count each row

        print(f"Document data exported successfully to {OUTPUT_CSV_FILE}")
        print(f"Total rows exported: {total_exported}")  # Show row count
        print(f"Ended: {datetime.datetime.now()}")
    except Exception as e:
        status = f'ERROR: {e}'
        print(f"Error during document export: {e}")
    finally:
        send_email_notification({
            'status': status,
            'script_start_time': script_start_time,
            'script_end_time': datetime.datetime.now(),
            'total_fetched': total_exported,  # Use the correct count here
            'file_loc': OUTPUT_CSV_FILE
        })
        if connection:
            connection.close()
            print("Database connection closed for history")

if __name__ == "__main__":
    export_to_csv()
