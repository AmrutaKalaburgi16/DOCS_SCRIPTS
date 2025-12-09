import cx_Oracle
import sys
import os
from util import send_email_notification
DB_USER = 'docspce'
DB_PASSWORD = 'Docs$2025'
DB_DSN = 'lllpdb.corp.intranet:1581/LLLP'


total_extracted=0

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
    
def export_table_as_inserts(table_name, output_file=None):
    """
    Export Oracle table data as INSERT statements
    
    Args:
        connection_string: Oracle connection string (e.g., 'username/password@localhost:1521/ORCL')
        table_name: Name of the table to export
        output_file: Optional output file path. If None, prints to console
    """
    connection = None
    try:
        # Connect to Oracle database
        connection =connect_to_oracle()
        cursor = connection.cursor()
        
        # Get table columns
        cursor.execute(f"""
            SELECT column_name, data_type 
            FROM all_tab_columns 
            WHERE table_name = UPPER('{table_name}')
            and owner='DOCS_OWNER'
            ORDER BY column_id
        """)
        columns = cursor.fetchall()
        
        if not columns:
            print(f"Table '{table_name}' not found or has no columns")
            return
        
        column_names = [col[0] for col in columns]
        column_types = {col[0]: col[1] for col in columns}
        
        # Fetch all data from the table
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        rows = cursor.fetchall()
        
        # Generate INSERT statements
        insert_statements = []
        for row in rows:
            values = []
            for i, value in enumerate(row):
                col_name = column_names[i]
                col_type = column_types[col_name]
                
                if value is None:
                    values.append('NULL')
                elif col_type in ('VARCHAR2', 'CHAR', 'CLOB', 'NVARCHAR2', 'NCHAR'):
                    # Escape single quotes
                    escaped_value = str(value).replace("'", "''")
                    values.append(f"'{escaped_value}'")
                elif col_type in ('DATE', 'TIMESTAMP'):
                    values.append(f"TO_DATE('{value}', 'YYYY-MM-DD HH24:MI:SS')")
                else:
                    values.append(str(value))
            
            column_list = ', '.join(column_names)
            values_list = ', '.join(values)
            insert_stmt = f"INSERT INTO {table_name} ({column_list}) VALUES ({values_list});"
            insert_statements.append(insert_stmt)
        
        total_extracted=len(insert_statements)
        # Output results
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                for stmt in insert_statements:
                    f.write(stmt + '\n')
            print(f"Exported {len(insert_statements)} rows to {output_file}")
        else:
            for stmt in insert_statements:
                print(stmt)
        
        # Close connection
        cursor.close()
        connection.close()
        
        print(f"\nTotal rows exported: {len(insert_statements)}")
        
     
    
    except cx_Oracle.Error as error:
        print(f"Oracle error: {error}")
    except Exception as e:
        print(f"Error: {e}")



def main():
    # Example usage
    if len(sys.argv) < 2:
        print("Usage: python export_oracle_to_insert.py  <table_name> [output_file]")
        print("Example: python export_oracle_to_insert.py EMPLOYEES output.sql")
        sys.exit(1)
    
    table_name = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    export_table_as_inserts(table_name, output_file)

send_email_notification({
    'status': 'SUCCESS' if total_extracted > 0 else 'NO_DATA',
    'total_extracted': total_extracted  # Use total_extracted, not row_count
}, __file__)

if __name__ == "__main__":
    main()
