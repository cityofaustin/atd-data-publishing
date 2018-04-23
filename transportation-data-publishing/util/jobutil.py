'''
Python client for logging job results via a postgrest interface.

This class logs the outcome of scripted tasks in a pre-configured database whose API
is made available via postgrest (https://postgrest.com/).

The CONGIG and instance parameters match the job database schema. That's what
it's there for.
'''
import pdb

import arrow
import requests


CONFIG = {
    'destination_field' : 'destination',
    'end_date_field' : 'end_date',
    'id_field' : 'id',
    'message_field' : 'message',
    'name_field' : 'name',
    'records_processed_field' : 'records_processed',
    'source_field' : 'source',
    'start_date_field' : 'start_date',
    'status_field' : 'status'
}


class Job(object):
    '''
    Class to interact with job control API.
    '''
    def __init__(self,
                 auth=None,
                 destination=None,
                 name=None,
                 url=None,
                 source=None):

        self.auth=auth  
        self.destination = destination
        self.name = name
        self.url = url
        self.source = source
              
        self.destination_field = CONFIG['destination_field']
        self.end_date_field = CONFIG['end_date_field']
        self.id_field = CONFIG['id_field']
        self.name_field = CONFIG['name_field']
        self.message_field = CONFIG['message_field']
        self.records_processed_field = CONFIG['records_processed_field']
        self.source_field = CONFIG['source_field']
        self.start_date_field = CONFIG['start_date_field']
        self.status_field = CONFIG['status_field']
        
        self.data=None


    def most_recent(self, status='success'):
        '''Return end date of the most-recent job run.'''

        url = f'{self.url}?{self.name_field}=eq.{self.name}&{self.status_field}=eq.{status}&order={self.end_date_field}.desc&limit=1'
        
        res = self._query('SELECT', url)

        try:
            return arrow.get(res[0][self.end_date_field]).timestamp
        
        except IndexError:
            return None


    def start(self):
        '''Start a new job with given name.'''
        data = {
            self.name_field : self.name,
            self.start_date_field : arrow.now().format(),
            self.end_date_field : None,
            self.status_field : 'in_progress',
            self.source_field : self.source,
            self.destination_field : self.destination,
        }

        self.data = self._query('INSERT', self.url, data=data)[0]
        return self.data


    def result(self,
               _result,
               message=None,
               records_processed=0):
        '''Update job status to specified result. '''

        if _result not in ['success', 'error']:
            raise Exception('Unknown result specified.')

        data = {
            self.id_field : self.data[self.id_field],
            self.end_date_field : arrow.now().format(),
            self.status_field : _result,
            self.message_field : message,
            self.records_processed_field : records_processed
        }

        self.data = self._query('UPDATE', self.url, data=data)[0]    
        return self.data


    def delete(self):
        '''Delete all job entries of specified name.'''

        print(f'''
            WARNING: You are about to delete all jobs with name {self.name}.
            ''')

        answer = input('Type \'Yes\' to continue: ')
        
        if answer.upper() == 'YES':
            url = f'{self.url}?{self.name_field}=eq.{self.name}'
            return self._query('DELETE', url)

        else:
            raise Exception('Delete aborted.')


    def _query(self, method, url, data=None):
        '''
        Private method to execute API calls.
        
        Returns response dict, which (if successful) is an array representation
        of the affected records (due to the header param return=representation).
        '''
        
        headers = {
            'Authorization': f'Bearer {self.auth}',
            'Content-Type' : 'application/json',
            'Prefer' : 'return=representation'  # return entire record json in response
        }

        if method.upper() == 'SELECT':
            res = requests.get(url, headers=headers)

        elif method.upper() == 'INSERT':
            res = requests.post(url, headers=headers, json=data)
        
        elif method.upper() == 'UPDATE':
            #  require ID match to prevent unintentional batch update
            _id = data.pop(self.id_field)
            url = f'{url}?id=eq.{_id}'

            res = requests.patch(url, headers=headers, json=data)
        
        elif method.upper() == 'DELETE':
            #  this will delete all rows that match query!
            res = requests.delete(self.url, headers=headers)

        else:
            raise Exception('Unknown method requested.')

        res.raise_for_status()
        return res.json()


#  Tests
if __name__ == '__main__' :
    import _setpath
    from config.secrets import *


    job = Job('test_job', JOB_DB_API_URL, 'test_source', 'test_dest', auth=JOB_DB_API_TOKEN)
    most_recent = job.most_recent()
    print(f'most recent: {most_recent}')
    print(job.start())
    print(job.result('success'))

    job = Job('test_job', JOB_DB_API_URL, 'test_source', 'test_dest', auth=JOB_DB_API_TOKEN)
    most_recent = job.most_recent()
    print(most_recent)
    print(f'most recent: {most_recent}')
    print(job.start())
    print(job.result('error', message='Something went wrong!'))

    job = Job('test_job', JOB_DB_API_URL, 'test_source', 'test_dest', auth=JOB_DB_API_TOKEN)
    job.delete()