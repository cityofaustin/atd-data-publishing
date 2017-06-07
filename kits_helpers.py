import logging
import pdb
import pymssql
import arrow

logger = logging.getLogger(__name__)

def generate_status_id_query(data, id_key):
    print('prep kits query')
    
    ids = [record[id_key] for record in data]
    
    where_ids = str(tuple(ids))
    
    query  = '''
        SELECT i.INTID as KITS_ID
            , e.DATETIME as OPERATION_STATE_DATETIME
            , e.STATUS as OPERATION_STATE
            , e.PLANID as PLAN_ID
            , i.ASSETNUM as ATD_SIGNAL_ID
            FROM [KITS].[INTERSECTION] i
            LEFT OUTER JOIN [KITS].[INTERSECTIONSTATUS] e
            ON i.[INTID] = e.[INTID]
            WHERE i.ASSETNUM IN {} AND e.DATETIME IS NOT NULL
            ORDER BY e.DATETIME DESC
    '''.format(where_ids)
    
    return query



def data_as_dict(creds, query):
    print('fetch kits data')

    conn = pymssql.connect(
        server=creds['server'],
        user=creds['user'],
        password=creds['password'],
        database=creds['database']
    )

    cursor = conn.cursor(as_dict=True)
    cursor.execute(query)  

    data = cursor.fetchall()

    return data



def check_for_stale(dataset, time_field, minute_tolerance):
    print('check for stale data')

    stale = False

    status_times = []

    for record in dataset:
        if record[time_field]:
            compare = arrow.get(record[time_field])
            status_times.append(compare)

    oldest_record =  arrow.get(max(status_times)).replace(tzinfo='US/Central')  #  set timezone because KITS timestamp does not have it

    delta = arrow.now() - oldest_record

    delta_minutes = delta.seconds/60

    print(str(delta_minutes))

    if delta_minutes > minute_tolerance:  #  if more than 15 minutes have passed since a status update

        stale = True

    return {'stale': stale, 'delta_minutes' : int(delta_minutes) }





def insert_multi_table(creds, query_array):
    print('multi-table insert in kits')
    
    conn = pymssql.connect(
        server=creds['server'],
        user=creds['user'],
        password=creds['password'],
        database=creds['database']
    )
    
    cursor = conn.cursor()
    
    for query in query_array:
        print(query)
        cursor.execute(query)

    conn.commit()
    conn.close()

    return 'done'
