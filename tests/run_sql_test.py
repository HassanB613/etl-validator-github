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
        for fname in os.listdir(sql_dir):
            if fname.endswith('.sql'):
                with open(os.path.join(sql_dir, fname), 'r') as f:
                    sql = f.read()
                    print(f'Running {fname}...')
                    cursor.execute(sql)
                    conn.commit()
        print('All SQL scripts executed successfully.')
except Exception as e:
    print(f'Error: {e}')
    exit(1)
