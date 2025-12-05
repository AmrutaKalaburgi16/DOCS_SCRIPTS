import cx_Oracle

# Source DB credentials
SRC_DB_USER = "docspce"
SRC_DB_PASSWORD = "Docs$2024"
SRC_DB_DSN = "lllpdb.corp.intranet:1581/LLLP"

# Dev DB credentials
DEV_DB_USER = "devapp10c"
DEV_DB_PASSWORD = "devapp10c"
DEV_DB_DSN = "racorad15-scan.corp.intranet:1521/ensdv01d_s"

SQL_QUERY = """
SELECT
    TASK_ID, DOCUMENT_ID, DOCUMENT_SUB_PATH, DOCUMENT_FILE_NAME,
    USER_CREATE_DATE, USER_MODIFY_DATE, FILE_TRANSFER_DATE,
    GCP_PATH, GCP_FILENAME, SEG_NUM, MATCH_CRITERIA, MATCH_DATA,
    TASK_CATEGORY_ID, ORIGINAL_FILE_NAME, DEPARTMENT_ID
FROM BSPD_DOCS_DOCUMENTS
where phase=3
"""

INSERT_QUERY = """
INSERT INTO BSPD_DOCS_DOCUMENTS (
        TASK_ID, DOCUMENT_ID, DOCUMENT_SUB_PATH, DOCUMENT_FILE_NAME,
USER_CREATE_DATE, USER_MODIFY_DATE, FILE_TRANSFER_DATE,
GCP_PATH, GCP_FILENAME, SEG_NUM, MATCH_CRITERIA, MATCH_DATA, TASK_CATEGORY_ID, ORIGINAL_FILE_NAME,
 DEPARTMENT_ID
) VALUES (
    :1, :2, :3, :4,
    :5, :6, :7, :8, :9,
    :10, :11, :12, :13, :14, :15
)
"""

def transfer_documents(batch_size=5000):
    src_conn = cx_Oracle.connect(SRC_DB_USER, SRC_DB_PASSWORD, SRC_DB_DSN)
    dev_conn = cx_Oracle.connect(DEV_DB_USER, DEV_DB_PASSWORD, DEV_DB_DSN)
    src_cursor = src_conn.cursor()
    dev_cursor = dev_conn.cursor()
    src_cursor.arraysize = batch_size

    src_cursor.execute(SQL_QUERY)
    batch_data = []
    total_inserted = 0

    while True:
        rows = src_cursor.fetchmany(batch_size)
        if not rows:
            break
        dev_cursor.executemany(INSERT_QUERY, rows)
        dev_conn.commit()
        total_inserted += len(rows)
        print(f"Inserted {total_inserted} records so far...")

    src_cursor.close()
    dev_cursor.close()
    src_conn.close()
    dev_conn.close()
    print(f"Data transfer complete. Total records inserted: {total_inserted}")

if __name__ == "__main__":
    transfer_documents