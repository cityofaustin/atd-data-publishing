import pyodbc
import csv
from secrets import IDB_PROD_CREDENTIALS  # or IDB_TEST_CREDENTIALS

QUERY = '''
    select * from Access.SYSTEM_INTERSECTIONS
'''

SOURCE_FILE = 'source-data/sync_systems_2016.csv'

def connect_db():
    print('connecting to db')

    conn = pyodbc.connect(
        'DRIVER={{SQL Server}};' 
            'SERVER={};'
            'PORT=1433;'
            'DATABASE={};'
            'UID={};'
            'PWD={}'
            .format(
                IDB_PROD_CREDENTIALS['server'],
                IDB_PROD_CREDENTIALS['database'],
                IDB_PROD_CREDENTIALS['user'],
                IDB_PROD_CREDENTIALS['password'] 
        ))

    return conn



def get_sql_data_as_dict(connection, query):
    print('getting sql data')

    results = []
    dict_results = {}

    cursor = connection.cursor()
    
    cursor.execute(query)

    columns = [column[0] for column in cursor.description]

    for row in cursor.fetchall():
        cursor.fetchall()
        results.append(dict(zip(columns, row)))
    
    for record in results:
        sys_id = str(int(record['SYSTEM_ID']))
        int_id = str(int(record['ATD_INTERSECTION_ID']))
        unique_id = sys_id + "$" + int_id

        dict_results[unique_id] = record

    return dict_results



def get_csv_data(source_file):
    print('getting csv data')

    with open(source_file, 'r') as file:
        reader = csv.DictReader(file)
        reader = [row for row in reader]
        return reader



def package_update(old, new):  #  update of any DB record that exists in the source data regardless of if different

    update = []

    for row in new:
        sys_id = row['SYSTEM_ID']
        int_id = row['ATD_INTERSECTION_ID']
        unique_id = sys_id + "$" + int_id
        
        if unique_id in old:
            row['ID'] = old[unique_id]['ID']
            update.append(row)

    return update



def update_database(connection, payload):
    print('updating {} records in database'.format(str(len(payload))))

    updated = 0

    cursor = connection.cursor()

    for record in payload:

        statement = '''
            UPDATE Access.SYSTEM_INTERSECTIONS
            SET ISOLATED='{}'
            WHERE ID='{}' 
        '''.format((record['ISOLATED']=='True'), record['ID'])

        cursor.execute(statement)

        updated += 1

    return updated

conn = connect_db()

sql_data = get_sql_data_as_dict(conn, QUERY)

csv_data = get_csv_data(SOURCE_FILE)

update_payload = package_update(sql_data, csv_data)

results = update_database(conn, update_payload)

print('{} records updated'.format(str(updated)))

