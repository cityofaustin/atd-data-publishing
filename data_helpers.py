import shutil
import pdb
import logging
from io import StringIO
import csv

import arrow
import pandas
import requests


logger = logging.getLogger(__name__)


def filter_by_key(data, key, val_list):
    print('filter by key {}'.format(key))
    #  filter a list of dictionaries by a list of key values
    #  http://stackoverflow.com/questions/29051573/python-filter-list-of-dictionaries-based-on-key-value
    return [d for d in data if d[key] in val_list]  


def filter_by_key_exists(data, key):
    'filter by key exists: {}'.format(key)
    #  return a list of dictionaries that have a specified key string
    #  http://stackoverflow.com/questions/29051573/python-filter-list-of-dictionaries-based-on-key-value
    return [d for d in data if key in d ]  


def add_missing_keys(list_of_dicts, list_of_keys, list_of_vals):
    print('add missing keys {}'.format(list_of_keys))
    #  look for keys in a list of dicts
    #  and missing keys and set to a default value
    #  case insensitive
    for d in list_of_dicts:
        for key in list_of_keys:
            if key not in d:
                index = list_of_keys.index(key)
                d[key] = list_of_vals[index]

    return list_of_dicts


def group_by_key(dataset, key):
    print('group data by {}'.format(key))

    grouped_data = {}
    
    for row in dataset:
        new_key = str(row[key])
        grouped_data[new_key] = row

    return grouped_data


def unique_keys(list_of_dicts):
    keys = [key for record in list_of_dicts for key in record]
    return list( set(keys) )


def list_key_values(list_of_dicts, key):
    #  generate a list of key values from a list of dictionaries
    if len(list_of_dicts) > 0:
        return [record[key] for record in list_of_dicts]

    else:
        return []


def upper_case_keys(list_of_dicts):
    upper = []

    for record in list_of_dicts:
        upper.append(dict((k.upper(), v) for k,v in record.items()))
    
    return upper


def lower_case_keys(list_of_dicts):
    lower = []

    for record in list_of_dicts:
        lower.append(dict((k.lower(), v) for k,v in record.items()))
    
    return lower


def stringify_key_values(list_of_dicts):
    print('stringify key values')
    stringified = []

    for record in list_of_dicts:
        stringified.append(dict((k, str(v).strip()) for k,v in record.items()))

    return stringified


def remove_linebreaks(list_of_dicts, list_of_keys):
    print('remove linebreaks')
    breakless = []

    for record in list_of_dicts:
        for key in list_of_keys:
            if key in record:
                if type(record[key]) is str:
                    record[key] = record[key].replace('\n', '')

        breakless.append(record)

    return breakless


def mills_to_unix(list_of_dicts):
    print('convert millesecond date to unix date')

    for record in list_of_dicts:
        for key in record:
            if '_DATE' in key.upper():
                milliseconds = float(record[key])
                record[key] = int( (milliseconds) / 1000 )

    return list_of_dicts



def unix_to_mills(list_of_dicts):
    print('convert unix dates to milleseconds')

    for record in list_of_dicts:
        for key in record:
            if '_DATE' in key.upper():
                milliseconds = float(record[key])
                record[key] = int( (milliseconds) * 1000 )

    return list_of_dicts


def iso_to_unix(list_of_dicts, **options):
    #  requires arrow
    #  convert ISO datetimes to unix
    print('convert ISO dates to unix')
    
    if 'replace_tz' not in options:
        options['replace_tz'] = False
    
    if 'tz_info' not in options:
        options['tz_info'] = 'US/Central'

    for record in list_of_dicts:
        for key in record:
            if '_DATE' in key.upper():
                if record[key]:
                    d = arrow.get(record[key])
                    
                    if options['replace_tz']:
                        d = d.replace(tzinfo=options['tz_info'])  #  timestamp is in local, so assign that info (true with KTIS) 

                    record[key] = str(d.timestamp)

    return list_of_dicts


def unix_to_iso(list_of_dicts, **options):
    #  requires arrow
    #  convert timestamps to unix
    #  ignores timestamps that cannot be converted to floats
    print('convert unix dates to ISO')

    if not 'out_format' in options:
        options['out_format'] = 'YYYY-MM-DDTHH:mm:ss'

    if not 'tz_info' in options:
        options['tz_info'] = 'US/Central'

    for record in list_of_dicts:
        for key in record:
            if '_DATE' in key.upper():
                if record[key]:

                    try:
                        timestamp = float(record[key])
                        #  we can't just use arrow.get(timestamp) here
                        #  because negative timestamps will fail in windows
                        #  so instead we shift from epoch by the timestamp value
                        d = arrow.get(0).shift(seconds=timestamp)
                                        
                        if 'replace_tz' in options:
                            d = d.replace(tzinfo=options['tz_info'])  #  timestamp is in local, so assign that info (true with KTIS) 

                        d = d.to(options['tz_info'])  #  timestamps in UTC, convert to local
                    
                        record[key] = d.format(options['out_format'])

                    
                    except ValueError:
                        print('{} not a valid unix timestamp'.format(record[key]))
                        continue

    return list_of_dicts


def merge_dicts(source_dicts, merge_dicts, join_key, merge_keys):
    #  insert fields from a merge dictionary to a source dictioary
    #  based on a matching key/val
    #  join field must exist in both source and merge dictionaries
    print('merge dicts')

    merged = []
    
    for merge_dict in merge_dicts:

        for source_dict in source_dicts: 
            
            if not join_key in merge_dict:
                continue
            
            if not join_key in source_dict:
                continue

            if str(merge_dict[join_key]) == str(source_dict[join_key]):

                for key in merge_dict:                

                    if key in merge_keys:
    
                        source_dict[key] = merge_dict[key]

                merged.append(source_dict)
                continue

    return merged


def detect_changes(old_data, new_data, join_key, **options):
    #  compare two list of dicts based on a unique join_key
    #  list keys in options['keys'] to specify which keys to compare

    print('detect changes between datasets')


    if 'keys' not in options:
        options['keys'] = []

    if 'addLatLonKeys' in options:
        if options['addLatLonKeys']:
            options['keys'] = options['keys'] + ['LATITUDE', 'LONGITUDE']

    change = []
    delete = []
    new = []
    no_change = []

    if new_data:        

        old_values = list_key_values(old_data, join_key)
        
        for old_record in old_data:  
            
            for new_record in new_data:
                if old_record[join_key] == new_record[join_key]:
                    break

            else:
                print('delete record: {}'.format(old_record[join_key]))
                delete.append(old_record)  #  delete old record if prim key not in new data

        for new_record in new_data:
            change_record = False

            if new_record[join_key] not in old_values:  #  new record if prim key not in old 
                print('new record: {}'.format(new_record[join_key]))
                new.append(new_record)
                continue

            for old_record in old_data:
                if new_record[join_key] == old_record[join_key]:  #  record match
                    
                    for key in new_record:  
                        
                        if options['keys']:  #  optionally ignore keys not specified in options
                            if key not in options['keys']:
                                continue

                        if key in old_record:  #  key exists in old
                            if new_record[key] != old_record[key]:  #  key/val unequal
                                print('unequal {}: {} new :{}'.format(key, old_record[key], new_record[key]))
                                print(new_record)
                                change_record = True
                                continue

                        if key not in old_record:  #  key in new data not in old data
                            print('new field: ' + key)
                            change_record = True
                            continue

                    for key in old_record:
                        if options['keys']:  #  optionally ignore keys not specified in options
                            if key not in options['keys']:
                                continue

                        if key not in new_record:  #  key in old data not in new data
                            print('key in old data not in new data: {} '.format(key))
                            new_record[key] = ''  #  must append empty key val or socrata will not modify
                            change_record = True
                            continue
                            
            if change_record:
                change.append(new_record)  #  change record
                
            else:
                no_change.append(new_record)  #  no change
                        
    else:
        delete = old_data  #  if no new data then flag all old data as delete

    return { 
        'change': change,
        'new': new,
        'no_change': no_change,
        'delete': delete,
    }


def concat_key_values(list_of_dicts, list_of_keys, new_key, join_string):
    print('concat keys {}'.format(list_of_keys))
    for d in list_of_dicts:
        concat =[]
            
        for key in list_of_keys:
            if not key in d:
                continue
            concat.append( str(d[key]) )

        d[new_key] = join_string.join(concat)

    return list_of_dicts


def group_by_unique_value(list_of_dicts, key):
    print('groupd by key {}'.format(key))
    grouped = {}
    
    for record in list_of_dicts:
        if record[key] not in grouped:
            grouped[record[key]] = [] 

        grouped[record[key]].append(record)

    return grouped


def sort_dicts_int(list_of_dicts, key):
    print('sort list of dicts')
    #  sort a list of dictionarys based on an integer key value
    #  http://stackoverflow.com/questions/72899/how-do-i-sort-a-list-of-dictionaries-by-values-of-the-dictionary-in-python
    return sorted(list_of_dicts, key=lambda k: int(k[key]), reverse=True) 


def max_index(list_of_vals, val):
    #  find the largest index of a value in a list
    #  http://stackoverflow.com/questions/6294179/how-to-find-all-occurrences-of-an-element-in-a-list
    return max([i for i, x in enumerate(list_of_vals) if x == val])

def min_index(list_of_vals, val):
    #  find the smallest index of a value in a list
    #  http://stackoverflow.com/questions/6294179/how-to-find-all-occurrences-of-an-element-in-a-list
    return min([i for i, x in enumerate(list_of_vals) if x == val])

def create_rank_list(list_of_dicts, rank_key_name):
    print('create rank list')
    #  create 'rank' key and assign rank based on position of dict in list
    for record in list_of_dicts:
        record[rank_key_name] = list_of_dicts.index(record) + 1 #  because list indices start at
    
    return list_of_dicts


def reduce_dicts(list_of_dicts, list_of_keys):
    #  del keys from dicts in a list of dicts
    #  put the keys you want to keep in list_of_keys
    print( 'reduce dictionaries to keys {}'.format(list_of_keys) )
    return [{ key: old_dict[key] for key in list_of_keys if key in old_dict} for old_dict in list_of_dicts]


def replace_keys(list_of_dicts, lookup_dict, **options):
    print('replace keys')

    if not 'delete_unmatched' in options:
        options['delete_unmatched'] = False

    unmatched_keys = []
    new_list_of_dicts = []

    for d in list_of_dicts:
        new_d = {}

        for key in d:
            if key in lookup_dict:
                new_d[lookup_dict[key]] = d[key]
            
            else:
                if key not in unmatched_keys:
                    unmatched_keys.append(key)
                
                if not options['delete_unmatched']:
                    new_d[key] = d[key]

        new_list_of_dicts.append(new_d)

    print('unmatched keys: {}'.format(str(unmatched_keys)))
    return new_list_of_dicts


def write_csv(data, **options):
    #  requires pandas
    #  requires arrow
    print('write data to file')

    if not 'in_memory' in options:
        options['in_memory'] = False

    if not 'file_name' in options:
        options['file_name'] = '{}'.format( arrow.now().timestamp )

    df = pandas.DataFrame(data)

    if options['in_memory']:
        return StringIO ( df.to_csv(index=False) )

    else:
        df.to_csv(options['file_name'], index=False)


def get_web_csv(url, **options):
    print('get CSV from web')

    if not 'encoding' in options:
        options['encoding'] = 'utf-8-sig'

    req = requests.get(url)
    req.encoding = options['encoding']
    file = StringIO(req.text)
    data = csv.DictReader(file)

    return data


def get_img(path, camera):
    logging.info('get_img {}'.format(path))
    print('get_img {}'.format(path))

    if 'IP' in camera:
        url = 'http://{}/jpeg'.format(camera['IP'])
    
    else:
        logging.warning('no IP for camera {}'.format(path))
        return

    try:
        res = requests.get(url, timeout=0.1, stream=True)
        
        if res.status_code == 200:
            with open(path, 'wb') as outfile:
                res.raw.decode_content = True
                shutil.copyfileobj(res.raw, outfile)

            del res

        else:
            logging.warning('status code {}'.format(res.text))

    except requests.exceptions.Timeout:
        logging.warning('timeout: {}'.format(url))
        return

    except requests.exceptions.RequestException:
        return