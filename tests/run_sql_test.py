import pyodbc
import configparser
import os

# Read config
config = configparser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), '../sqlUtils/sqlconfig.ini'))

server = config['Credentials']['Server']
database = config['Credentials']['Database']
username = config['Credentials']['UserName']
password = config['Credentials']['Password']

# Connect to SQL Server
conn_str = (
    f'DRIVER={{ODBC Driver 17 for SQL Server}};'
    f'SERVER={server};'
    f'DATABASE={database};'
    f'UID={username};'
    f'PWD={password}'
)

try:
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        # Run all .sql files in the sqlUtils folder
        sql_dir = os.path.join(os.path.dirname(__file__), '../sqlUtils')
        sql_files = sorted([f for f in os.listdir(sql_dir) if f.endswith('.sql')])
        
        if not sql_files:
            print('No SQL files found in sqlUtils folder.')
        
        for fname in sql_files:
            with open(os.path.join(sql_dir, fname), 'r') as f:
                sql = f.read()
                print(f'\n=== Running {fname} ===')
                
                # Execute SQL and fetch results if available
                cursor.execute(sql)
                
                # Try to fetch results if it's a SELECT statement
                try:
                    rows = cursor.fetchall()
                    if rows:
                        # Print column names
                        columns = [column[0] for column in cursor.description]
                        print(f"Columns: {', '.join(columns)}")
                        # Print rows
                        for row in rows:
                            print(row)
                    else:
                        print('Query executed successfully (no results returned)')
                except pyodbc.ProgrammingError:
                    # Not a SELECT statement or no results
                    conn.commit()
                    print('Query executed successfully')
        
        print('\n✅ All SQL scripts executed successfully.')
except Exception as e:
    print(f'❌ Error: {e}')
    exit(1)
