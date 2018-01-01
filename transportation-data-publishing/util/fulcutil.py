''' 
Helper methods to interact with a Fulcrum app.

See:
    http://developer.fulcrumapp.com/
    https://github.com/fulcrumapp/fulcrum-python
'''
import pdb
from fulcrum import Fulcrum
import requests


def get_all_metadata(api_key):
    forms = Fulcrum.forms.search()
    return forms['forms']


def get_users(api_key, form_id):
    fulcrum = Fulcrum(key=api_key)
    users = fulcrum.memberships.search(url_params={'form_id': form_id}) 
    return users['memberships']


# def get_user_by_email(api_key, form_id, email):
#     email = email.lower()
#     fulcrum = Fulcrum(key=api_key_fulcrum)
#     users = fulcrum.memberships.search(url_params={'form_id': form_id})

#     for user in users['memberships']:
#         if user['email'].lower() == email:
#             return user

#     return None


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
















