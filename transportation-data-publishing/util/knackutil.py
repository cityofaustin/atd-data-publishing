'''
Helper methods for interacting with the Knack API.

See: https://www.knack.com/developer-documentation/
And: https://github.com/cityofaustin/knackpy
'''
import arrow


def date_filter_on_or_after(date_str, date_field):
    '''
    Return a Knack filter to retrieve records that have been
    modified since the last job run
    '''
    
    #  knack date filter requires MM/DD/YYYY
    date_str = arrow.get(date_str).format('MM/DD/YYYY')

    return {
        'match': 'or',
        'rules': [
                {
                    'field':f'{date_field}',
                    'operator':'is',
                    'value':f'{date_str}'
                },
                {
                    'field':f'{date_field}',
                    'operator':'is after',
                    'value':f'{date_str}'
                },

            ]
    }



def attachment_url(records, in_fieldname='ATTACHMENT', out_fieldname='ATTACHMENT_URL'):
    for record in records:
        attachment = record.get(in_fieldname)
        
        if attachment:
            record[out_fieldname] = attachment.get('url')

    return records









