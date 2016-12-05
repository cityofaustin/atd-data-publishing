from operator import itemgetter
from collections import defaultdict
import itertools
import arrow
import pdb



def FilterbyKey(data, key, val_list):
    print('filter by key {}'.format(key))
    #  filter a list of dictionaries by a list of key values
    #  http://stackoverflow.com/questions/29051573/python-filter-list-of-dictionaries-based-on-key-value
    return [d for d in data if d[key] in val_list]  



def FilterbyKeyExists(data, key):
    'filter by key exists: {}'.format(key)
    #  return a list of dictionaries that have a specified key string
    #  http://stackoverflow.com/questions/29051573/python-filter-list-of-dictionaries-based-on-key-value
    return [d for d in data if key in d]  



def GroupByKey(dataset, key):
    print('group data by {}'.format(key))

    grouped_data = {}
    
    for row in dataset:
        new_key = str(row[key])
        grouped_data[new_key] = row

    return grouped_data



def ListKeyValues(list_of_dicts, key):
    #  generate a list of key values from a list of dictionaries
    if len(list_of_dicts) > 0:
        return [record[key] for record in list_of_dicts]

    else:
        return []



def UpperCaseKeys(list_of_dicts):
    upper = []

    for record in list_of_dicts:
        upper.append(dict((k.upper(), v) for k,v in record.items()))
    
    return upper



def LowerCaseKeys(list_of_dicts):
    lower = []

    for record in list_of_dicts:
        lower.append(dict((k.lower(), v) for k,v in record.items()))
    
    return lower



def StringifyKeyValues(list_of_dicts):
    print('stringify key values')
    stringified = []

    for record in list_of_dicts:
        stringified.append(dict((k, str(v).strip()) for k,v in record.items()))

    return stringified



def ConvertMillsToUnix(list_of_dicts):
    print('convert millesecond date to unix date')
    for record in list_of_dicts:
        for key in record:
            if '_DATE' in key.upper():
                milliseconds = float(record[key])
                record[key] = int( (milliseconds) / 1000 )

    return list_of_dicts




def ConvertUnixToMills(list_of_dicts):
    print('convert unix dates to milleseconds')
    for record in list_of_dicts:
        for key in record:
            if '_DATE' in key.upper():
                milliseconds = float(record[key])
                record[key] = int( (milliseconds) * 1000 )

    return list_of_dicts


def ConvertISOToUnix(list_of_dicts):
    print('convert ISO dates to unix')
    for record in list_of_dicts:
        for key in record:
            if '_DATE' in key.upper():
                if record[key]:
                    d = arrow.get(record[key])
                    record[key] = str(d.timestamp)

    return list_of_dicts



def ConvertUnixToISO(list_of_dicts, **options):
    print('convert unix dates to ISO')

    if not 'out_format' in options:
        options['out_format'] = 'YYYY-MM-DDTHH:mm:ss'

    if not 'tz_info' in options:
        options['tz_info'] = 'US/Central'

    if not 'in_format' in options:
        options['in_format'] = 'X'  #  http://crsmithdev.com/arrow/#tokens

    for record in list_of_dicts:
        for key in record:
            if '_DATE' in key.upper():
                if record[key]:
                    d = arrow.get(str(record[key]), options['in_format'] ).replace(tzinfo=options['tz_info'])
                    record[key] = d.format(options['out_format'])

    return list_of_dicts



def MergeDicts(source_dicts, merge_dicts, join_key, merge_keys):
    #  insert fields from a merge dictionary to a source dictioary
    #  based on a matching key/val
    #  join field must exist in both source and merge dictionaries
    print('merge dicts')

    merged = []
    
    for merge_dict in merge_dicts:

        for source_dict in source_dicts: 
            
            if str(merge_dict[join_key]) == str(source_dict[join_key]):

                for key in merge_dict:                

                    if key in merge_keys:
    
                        source_dict[key] = merge_dict[key]

                merged.append(source_dict)
                continue

    return merged



def DetectChanges(old_data, new_data, join_key, **options):
    #  compare two list of dicts based on a unique join_key
    #  list keys in options['keys'] to specify which keys to compare

    print('detect changes between datasets')


    if 'keys' not in options:
        options['keys'] = []

    change = []
    delete = []
    new = []
    no_change = []

    if new_data:        

        old_keys = ListKeyValues(old_data, join_key)
        
        for old_record in old_data:  
            
            for new_record in new_data:
                if old_record[join_key] == new_record[join_key]:
                    break

            else:
                print('delete record: {}'.format(old_record[join_key]))
                delete.append(old_record)  #  delete old record if prim key not in new data

        for new_record in new_data:
            if new_record[join_key] not in old_keys:  #  new record if prim key not in old 
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
                                change.append(new_record)  #  change record
                                break

                        if key not in old_record:  #  key in new data not in old data
                            print('new field: ' + key)
                            change.append(new_record)  #  change record
                            break
                    else:
                        for key in old_record:
                            if options['keys']:  #  optionally ignore keys not specified in options
                                if key not in options['keys']:
                                    continue

                            if key not in new_record:  #  key in old data not in new data
                                print('key in old data not in new data')
                                change.append(new_record)  #  change record
                                break

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


def ConcatKeyVals(list_of_dicts, list_of_keys, new_key, join_string):
    print('concat keys {}'.format(list_of_keys))
    for d in list_of_dicts:
        concat =[]
            
        for key in list_of_keys:
            if not key in d:
                continue
            concat.append( str(d[key]) )

        d[new_key] = join_string.join(concat)

    return list_of_dicts



def GroupByUniqueValue(list_of_dicts, key):
    print('groupd by key {}'.format(key))
    grouped = {}
    
    for record in list_of_dicts:

        if record[key] not in grouped:
            grouped[record[key]] = [] 

        grouped[record[key]].append(record)

    return grouped



def SortDictsInt(list_of_dicts, key):
    print('sort list of dicts')
    #  sort a list of dictionarys based on an integer key value
    #  http://stackoverflow.com/questions/72899/how-do-i-sort-a-list-of-dictionaries-by-values-of-the-dictionary-in-python
    return sorted(list_of_dicts, key=lambda k: int(k[key]), reverse=True) 
    


def createRankList(list_of_dicts, rank_key_name):
    print('create rank list')
    #  create 'rank' key and assign rank based on position of dict in list
    for record in list_of_dicts:
        record[rank_key_name] = list_of_dicts.index(record) + 1 #  because list indices start at
    
    return list_of_dicts



def ReduceDicts(list_of_dicts, list_of_keys):
    #  del keys from dicts in a list of dicts
    #  put the keys you want to keep in list_of_keys
    print( 'reduce dictionary to keys {}'.format(list_of_keys) )
    out_list_of_dicts = []
    
    for d in list_of_dicts:
        temp = {}
        for key in d:
            if key in list_of_keys:
                temp[key] = d[key]

        out_list_of_dicts.append(temp)

    return out_list_of_dicts



def ReplaceDictKeys(list_of_dicts, lookup_dict):
    print('replace keys')

    for d in list_of_dicts:
        for key in d:
            if key in lookup_dict:
                d[lookup_dict[key]] = d.pop(key)

    return list_of_dicts













