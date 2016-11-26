def FilterbyKey(data, key, val_list):
    #  filter a list of dictionaries by a list of key values
    #  http://stackoverflow.com/questions/29051573/python-filter-list-of-dictionaries-based-on-key-value
    return [d for d in data if d[key] in val_list]  



def GroupByKey(dataset, key):
    print('group data by {}'.format(key))

    grouped_data = {}
    
    for row in dataset:
        new_key = str(row[key])
        grouped_data[new_key] = row

    return grouped_data



def ListKeyValues(list_of_dicts, key):
    #  generate a list of key values from a list of dictionaries
    return [record[key] for record in list_of_dicts]



def UpperCaseKeys(list_of_dicts):
    upper = []

    for record in list_of_dicts:
        upper.append(dict((k.upper(), v) for k,v in record.items()))
    
    return upper



def StringifyKeyValues(list_of_dicts):
    stringified = []

    for record in list_of_dicts:
        stringified.append(dict((k, str(v)) for k,v in record.items()))

    return stringified



def MergeDicts(source_dict, merge_dict, join_field, merge_fields):
    #  insert fields from a merge dictionary to a source dictioary
    #  based on a matching key/val
    #  join field must exist in both source and merge dictionaries
    print('merge data')

    not_in_merger = []

    count = 0

    source_keys = ListKeyValues(source_dict, join_field)
    merge_keys = ListKeyValues(merge_dict, join_field)
    
    
    for key in source_keys:

        if key in merge_keys:
            for field in merge_fields:
                source_dict[key][field] = merge_dict[key][field]

        else:
            not_in_merger.append(key)
            del source_dict[key]

    print(str(len(not_in_merger)) + " records not found in merge dictionary!")

    return {
        'merged_data': source_dict,
        'not_found': not_in_merger
    }



def DetectChanges(old_data, new_data, join_key):
    #  compare two list of dicts based on a unique join_key
    #  only compare keys present in both dicts
    print('detect changes between datasets')

    change = []
    delete = []
    new = []
    no_change = []

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

    return { 
        'change': change,
        'new': new,
        'no_change': no_change,
        'delete': delete,
    }
