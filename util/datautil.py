'''
Handy data-munging utilities.
'''
import csv
from io import StringIO
import shutil
import pdb

import arrow
import requests


def filter_by_val(dicts, key, val_list):
    #  filter a list of dictionaries by a list of key values
    #  assumes input dicts contain relevant keys
    #  http://stackoverflow.com/questions/29051573/python-filter-list-of-dictionaries-based-on-key-value
    return [d for d in dicts if d[key] in val_list]  

def reduce_to_keys(dicts, keys):
    #  reduce a list of dictionaries by dropping entries
    #  that are not in a specified list of keys
    reduced = []
    for d in dicts:
        d_new = {}
        for key in d.keys():
            if key in keys:
                d_new[key] = d[key]

        reduced.append(d_new)
    return reduced

def filter_by_key_exists(dicts, key):
    #  return a list of dictionaries that have a specified key string
    #  http://stackoverflow.com/questions/29051573/python-filter-list-of-dictionaries-based-on-key-value
    return [d for d in dicts if key in d]

def add_missing_keys(dicts, key_vals):
    #  look for keys in a list of dicts
    #  and missing keys and set to a default value
    for d in dicts:
        for key in key_vals.keys():
            if key not in d:
                d[key] = key_vals[key]

    return dicts

def unique_keys(dicts):
    keys = [key for record in dicts for key in record]
    return list( set(keys) )

def get_key_values(dicts, key):
    #  generate a list of key values from a list of dictionaries
    return [record[key] for record in dicts]

def lower_case_list(l):
    return [e.lower() for e in l]

def lower_case_keys(dicts):
    return [ { k.lower() : v for k,v in record.items() } for record in dicts ]

def upper_case_keys(dicts):
    return [ { k.upper() : v for k,v in record.items() } for record in dicts ]

def stringify_key_values(dicts):
    return [ { k : str(v).strip() for k,v in record.items() } for record in dicts ] 

def remove_linebreaks(dicts, keys):
    print('remove linebreaks')
    breakless = []

    for record in dicts:
        for key in keys:
            if key in record:
                try:
                    record[key] = record[key].replace('\n', '')
                except ValueError:
                    continue

        breakless.append(record)

    return breakless

def mills_to_unix(dicts, keys):
    print('convert millesecond date to unix date')

    for record in dicts:
        for key in record:
            if key in keys:
                try:
                    milliseconds = float(record[key])
                    record[key] = int( (milliseconds) / 1000 )
                except ValueError:
                    #  handle empty values
                    if not record[key]:
                        continue
                    else:
                        raise ValueError

    return dicts

def mills_to_iso(dicts, keys, tz='US/Central'):
    print('convert millesecond date to ISO8601 date')

    for record in dicts:
        for key in record:
            if key in keys:
                try:
                    unix = float(record[key]) / 1000
                    utc = arrow.get(0).shift(seconds=unix)
                    local = utc.to(tz)
                    record[key] = local.format()
                
                except ValueError:
                    #  handle empty values
                    if not record[key]:
                        continue
                    else:
                        raise ValueError
    return dicts

def unix_to_mills(dicts):
    print('convert unix dates to milleseconds')

    for record in dicts:
        for key in record:
            if key in keys:
                try:
                    milliseconds = float(record[key])
                    record[key] = int( (milliseconds) * 1000 )
    
                except ValueError:
                    #  handle empty values
                    if not record[key]:
                        continue
                    else:
                        raise ValueError
    return dicts

def iso_to_unix(dicts, keys):
    #  requires arrow
    #  convert ISO datetimes to unix
    print('convert ISO dates to unix')
    
    for record in dicts:
        for key in record:
            if key in keys:
                try:
                    d = arrow.get(record[key])
                    record[key] = str(d.timestamp)
                except ValueError:
                    #  handle empty values
                    if not record[key]:
                        continue
                    else:
                        raise ValueError
    return dicts

def unix_to_iso(dicts, **options):
    #  requires arrow
    #  convert timestamps to unix
    #  ignores timestamps that cannot be converted to floats
    print('convert unix dates to ISO')

    if not 'out_format' in options:
        options['out_format'] = 'YYYY-MM-DDTHH:mm:ss'

    for record in dicts:
        for key in record:
            if '_DATE' in key.upper():
                if record[key]:

                    try:
                        timestamp = float(record[key])
                        #  we can't just use arrow.get(timestamp) here
                        #  because negative timestamps will fail in windows
                        #  so instead we shift from epoch by the timestamp value
                        d = arrow.get(0).shift(seconds=timestamp)
                                        
                        record[key] = d.format(options['out_format'])
                    
                    except ValueError:
                        print('{} not a valid unix timestamp'.format(record[key]))
                        continue

    return dicts

def replaceTimezone(dicts, tz='US/Central', in_format='unix'):
    '''
    replace the timzone of a 'naive' timestamp with its timezone
    '''
    for record in dicts:
        for key in record:
            if 'DATE' in key.upper():
                if record[key]:
                    if in_format in ['unix', 'iso']:
                        #  arrow can parse unix or iso
                        d = arrow.get(record[key])

                    elif in_format == 'mills':
                        unix = int(record[key]) / 1000
                        d = arrow.get(unix)
                    else:
                        raise Exception('Invalid date format specified')

                    record[key] = d.replace(tzinfo=tz)

    return dicts

def merge_dicts(source_dicts, merge_dicts, join_key, merge_keys):
    #  insert specified fields from a merge dictionary to a source dictionary
    #  based on a matching key/val
    #  join field must exist in both source and merge dictionaries
    print('merge dicts')

    merged = []
    
    for merge_dict in merge_dicts:

        for source_dict in source_dicts: 
            
            try:
                if str(merge_dict[join_key]) == str(source_dict[join_key]):

                    for key in merge_dict:                

                        if key in merge_keys:
        
                            source_dict[key] = merge_dict[key]

                    merged.append(source_dict)
                    continue
                    
            except KeyError:
                continue

    return merged


def detect_changes(old_data, new_data, join_key, **options):
    #  compare two list of dicts based on a unique join_key
    #  list keys in options['keys'] to specify which keys to compare
    if 'keys' not in options:
        options['keys'] = []

    change = []
    delete = []
    new = []
    no_change = []

    if new_data:        

        #  get all 'old' primary keys
        old_values = get_key_values(old_data, join_key)
        
        for old_record in old_data:  
            #  verify old records exist in new data
            for new_record in new_data:
                if old_record[join_key] == new_record[join_key]:
                    break

            else:
                #  delete old record if prim key not in new data
                print('Delete record: {}'.format(old_record[join_key]))
                delete.append(old_record)  

        for new_record in new_data:
            change_record = False

            if new_record[join_key] not in old_values:  
                #  new record if prim key not in old 
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
                            new_record[key] = ''  #  append empty key val for upload
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


def concat_key_values(dicts, keys, new_key, join_string):
    '''
    concatenate multiple dict fields into a new field
    '''
    print('concat keys {}'.format(keys))
    for d in dicts:
        concat =[]
            
        for key in keys:
            if not key in d:
                continue
            concat.append( str(d[key]) )

        d[new_key] = join_string.join(concat)

    return dicts


def group_by_unique_value(dicts, key):
    print('groupd by key {}'.format(key))
    grouped = {}
    
    for record in dicts:
        if record[key] not in grouped:
            grouped[record[key]] = [] 

        grouped[record[key]].append(record)

    return grouped


def sort_dicts_int(dicts, key):
    print('sort list of dicts')
    #  sort a list of dictionarys based on an integer key value
    #  http://stackoverflow.com/questions/72899/how-do-i-sort-a-list-of-dictionaries-by-values-of-the-dictionary-in-python
    return sorted(dicts, key=lambda k: int(k[key]), reverse=True) 


def max_index(list_of_vals, val):
    #  find the largest index of a value in a list
    #  http://stackoverflow.com/questions/6294179/how-to-find-all-occurrences-of-an-element-in-a-list
    return max([i for i, x in enumerate(list_of_vals) if x == val])

def min_index(list_of_vals, val):
    #  find the smallest index of a value in a list
    #  http://stackoverflow.com/questions/6294179/how-to-find-all-occurrences-of-an-element-in-a-list
    return min([i for i, x in enumerate(list_of_vals) if x == val])

def create_rank_list(dicts, rank_key_name):
    print('create rank list')
    #  create 'rank' key and assign rank based on position of dict in list
    for record in dicts:
        record[rank_key_name] = dicts.index(record) + 1 #  because list indices start at
    
    return dicts

def remove_empty_entries(dicts):
    #  drop keys from dicts in a list of dicts
    #  if key is falsey.
    reduced = []
    for d in dicts:
        new_d = {}
        for key in d:
            if d[key]:
                new_d[key] = d[key]
        reduced.append(new_d)
    return reduced

def replace_keys(dicts, lookup_dict):
    print('Replace keys')
    unmatched_keys = []
    new_dicts = []

    for d in dicts:
        new_d = {}

        for key in d:
            if key in lookup_dict:
                new_d[lookup_dict[key]] = d[key]
            
            else:
                new_d[key] = d[key]

        new_dicts.append(new_d)

    return new_dicts
    

def get_web_csv(url, encoding='utf-8-sig'):
    req = requests.get(url)
    req.encoding = options['encoding']
    file = StringIO(req.text)
    data = csv.DictReader(file)

    return data


def get_cctv_img(path, camera):
    print('get_img {}'.format(path))

    if 'IP' in camera:
        url = 'http://{}/jpeg'.format(camera['IP'])
    
    else:
        return

    try:
        res = requests.get(url, timeout=0.1, stream=True)
        
        if res.status_code == 200:
            with open(path, 'wb') as outfile:
                res.raw.decode_content = True
                shutil.copyfileobj(res.raw, outfile)

            del res

    except requests.exceptions.Timeout:
        return

    except requests.exceptions.RequestException:
        return

