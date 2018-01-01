import logging
import pdb

import arrow
import pymssql


def get_conn(creds, max_tries=5):
    if max_tries > 15:
        raise Exception('Retry limit is 15')

    attempts = 0

    while attempts <= max_tries:
        #  try to connect to database and retry as needed
        attempts += 1

        try:
            conn = pymssql.connect(
                server=creds['server'],
                user=creds['user'],
                password=creds['password'],
                database=creds['database'],
                timeout=10
            )

        except pymssql.OperationalError as e:
            #  handle error unless max tries exceeded
            if 'Adaptive Server connection failed' in str(e) and attempts < max_tries:
                continue
            else:
                raise e

        return conn


def status_query():
   return '''
        SELECT i.INTID as KITS_ID
        , e.DATETIME as OPERATION_STATE_DATETIME
        , e.STATUS as OPERATION_STATE
        , e.PLANID as PLAN_ID
        , i.ASSETNUM as SIGNAL_ID
        FROM [KITS].[INTERSECTION] i
        LEFT OUTER JOIN [KITS].[INTERSECTIONSTATUS] e
        ON i.[INTID] = e.[INTID]
        WHERE e.DATETIME IS NOT NULL
        ORDER BY e.DATETIME DESC
    '''


def data_as_dict(creds, query, max_tries=5):
    conn = get_conn(creds, max_tries=max_tries)
    cursor = conn.cursor(as_dict=True)   
    cursor.execute(query)  
    data = cursor.fetchall()
    conn.close()
    return data


def check_for_stale(dataset, time_field, minute_tolerance):
    print('check for stale data')

    stale = False

    status_times = []

    for record in dataset:
        if record[time_field]:
            compare = arrow.get(record[time_field])
            status_times.append(compare)

    oldest_record =  arrow.get(max(status_times))

    delta = arrow.now() - oldest_record

    delta_minutes = delta.seconds/60

    print(str(delta_minutes))

    if delta_minutes > minute_tolerance:  #  if more than 15 minutes have passed since a status update

        stale = True

    return {'stale': stale, 'delta_minutes' : int(delta_minutes) }


def insert_multi_table(creds, query_array, max_tries=5):
    print('multi-table insert in kits')
    
    conn = get_conn(creds, max_tries=max_tries)
    
    cursor = conn.cursor()
    
    for query in query_array:
        print(query)
        cursor.execute(query)

    conn.commit()
    conn.close()

    return 'done'
