from operator import itemgetter
from collections import defaultdict

def FilterbyKey(data, key, val_list):
    #  filter a list of dictionaries by a list of key values
    #  http://stackoverflow.com/questions/29051573/python-filter-list-of-dictionaries-based-on-key-value
    return [d for d in data if d[key] in val_list]  



def FilterbyKeyExists(data, key):
    #  filter a list of dictionaries by a list of key values
    #  http://stackoverflow.com/questions/29051573/python-filter-list-of-dictionaries-based-on-key-value
    return [d for d in data if d[key]]  



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
    stringified = []

    for record in list_of_dicts:
        stringified.append(dict((k, str(v)) for k,v in record.items()))

    return stringified



def MergeDicts(source_dicts, merge_dicts, join_key, merge_keys):
    #  insert fields from a merge dictionary to a source dictioary
    #  based on a matching key/val
    #  join field must exist in both source and merge dictionaries
    print('merge data')

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



def DetectChanges(old_data, new_data, join_key):
    #  compare two list of dicts based on a unique join_key
    #  only compare keys present in both dicts
    print('detect changes between datasets')

    change = []
    delete = []
    new = []
    no_change = []


    if new_data:        

        old_keys = ListKeyValues(old_data, join_key)
        new_keys = ListKeyValues(new_data, join_key)
        
        for old_record in old_data:  #  delete record
            
            for new_record in new_data:
                if old_record[join_key] == new_record[join_key]:
                    break

            else:
                delete.append(old_record)

        for new_record in new_data:
            if new_record[join_key] not in old_keys:  #  new record
                new.append(new_record)
                continue

            for old_record in old_data:
                if new_record[join_key] == old_record[join_key]:  #  record match
                    
                    for key in new_record:
                        if key in old_record:  #  key exists in old
                            
                            if new_record[key] != old_record[key]:  #  key/val unequal
                                change.append(new_record)  #  change record
                                break

                    else:
                        no_change.append(new_record)  #  no change
                        break

    else:
        delete = old_data

    return { 
        'change': change,
        'new': new,
        'no_change': no_change,
        'delete': delete,
    }


def ConcatKeyVals(list_of_dicts, list_of_keys, new_key, join_string):

    for d in list_of_dicts:
        concat =[]
            
        for key in list_of_keys:
            if not d[key]:
                continue
            concat.append( str(d[key]) )

        d[new_key] = join_string.join(concat)

    return list_of_dicts



def GroupByUniqueValue(list_of_dicts, key):
    grouped = {}
    
    for record in list_of_dicts:

        if record[key] not in grouped:
            grouped[record[key]] = [] 

        grouped[record[key]].append(record)

    return grouped



def SortDictsInt(list_of_dicts, key):
    #  sort a list of dictionarys based on an integer value
    #  http://stackoverflow.com/questions/72899/how-do-i-sort-a-list-of-dictionaries-by-values-of-the-dictionary-in-python
    return sorted(list_of_dicts, key=lambda k: int(k[key]), reverse=True) 
    


def createRankList(list_of_dicts):
    #  create 'rank' key and assign rank based on position of dict in list
    for record in list_of_dicts:
        record['RANK'] = list_of_dicts.index(record) + 1 #  because list indices start at
    
    return list_of_dicts



def ReduceDicts(list_of_dicts, list_of_keys):
    #  del keys from dicts in a list of dicts
    #  put the keys you want to keep in list_of_keys
    out_list_of_dicts = []
    
    for d in list_of_dicts:
        temp = {}
        for key in d:
            if key in list_of_keys:
                temp[key] = d[key]

        out_list_of_dicts.append(temp)

    return out_list_of_dicts