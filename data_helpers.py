def FilterbyKey(data, key, val_list):
    #  filter a list of dictionaries by a list of key values
    #  http://stackoverflow.com/questions/29051573/python-filter-list-of-dictionaries-based-on-key-value
    return [d for d in data if d[key] in val_list]  



def GroupByKey(dataset, key):
    print('group data by '{}.format(key))

    grouped_data = {}
    
    for row in dataset:
        new_key = str(row[key])
        grouped_data[new_key] = row

    return grouped_data



def ListKeyValues(list_of_dicts, key):
    #  generate a list of key values from a list of dictionaries
    return [record[key] for record in list_of_dicts]



def MergeDicts(source_dict, merge_dict, join_field, merge_fields):
    #  insert fields from a merge dictionary to a source dictioary
    #  based on a matching key/val
    #  join field must exist in both source and merge dictionaries
    print('merge data')

    not_in_merger = []

    count = 0

    source_key_list = ListKeyValues(source_dict, join_field)
    merge_key_list = ListKeyValues(merge_dict, join_field)
    
    
    for key in source_key_list:

        if key in merge_dict:
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




def DetectChanges(new, old, join_key, compare_key):
    print('detect changes and generate historical list')

    changed_records = []  #  see https://dev.socrata.com/publishers/changed.html
    new_records = []
    not_processed = []
    no_update = 0  
    new_count = 0
    update = 0
    delete = 0    
    changed_historical = []

    for record in new:
        lookup = str(new[record][join_key])

        if lookup in old:
            new_value = str(new[record][compare_key])

            try:
                old_value = str(old[lookup][compare_key])

            except:
                not_processed.append(new[record][join_key])
                continue
            
            if new_value == old_value:
                no_update += 1
            
            else:
                
                new[record][compare_key + '_previous'] = old_value
                
                changed_records.append(new[record])
                
                record_retired_datetime = arrow.now()
                old[lookup]['record_retired_datetime'] = record_retired_datetime.format('YYYY-MM-DD HH:mm:ss')

                processed_datetime = arrow.get(old[lookup]['processed_datetime']).replace(tzinfo='US/Central')

                delta = record_retired_datetime - processed_datetime
                old[lookup][ compare_key + '_duration'] = delta.seconds
                
                changed_historical.append(old[lookup])
            
        else:
            new_records.append(new[record])

    for record in old:  #  compare socrata to KITS to idenify deleted records
        lookup = old[record]['atd_signal_id']
        
        if lookup not in new:
            delete += 1

            changed.append({ 
                'atd_signal_id': lookup,
                ':deleted': True
            })

    return { 
        'changed': changed,
        'not_processed': not_processed,
        'new_records': new_records,
        'new': new,
        'update': len(changed),
        'no_update':  no_update,
        'delete': delete,
        'changed_historical': changed_historical
    }