''' 
Helper methods to interact with a Fulcrum app.

See:
    http://developer.fulcrumapp.com/
    https://github.com/fulcrumapp/fulcrum-python
'''
import pdb
from fulcrum import Fulcrum
import requests


def get_field_data(fulc, form_id):
    form = fulcrum.forms.find(form_id_fulcrum)
    return form['form'][0]['elements']


def get_query_by_value(field, value, table):
    # return f'SELECT * from "{table}" WHERE {field} LIKE "{value}"'
    return f'SELECT * from "{table}" WHERE {field} LIKE \'{value}\''


def query(api_key, query, format_='json', timeout=15):
    url = 'https://api.fulcrumapp.com/api/v2/query'
    params = {
        'token' : api_key,
        'q' : query,
        'format' : format_
    }
    res = requests.get(url, params=params, timeout=timeout)
    
    return res.json()


def get_template():
    ''' fulcrum record dict with required keys '''
    return  { 'record': {
            'form_id': None,
            'form_values': {},
            # 'id',    <<< update only
        }
    }


def format_record(record, template, form_id):
    template['record']['form_id'] = form_id
    return template
















