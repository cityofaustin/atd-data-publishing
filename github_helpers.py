from StringIO import StringIO
import arrow
import requests
import json
import csv
import base64
from secrets import GITHUB_CREDENTIALS

GITHUB_AUTH = (GITHUB_CREDENTIALS['user'], GITHUB_CREDENTIALS['token'])



def fetch_logfile_data(data_url, filename):
    print('fetch logfile data')
    
    headers = {'Cache-Control': 'no-store'}

    res = requests.get(data_url + filename, headers=headers)
    return str(res.text)
 
 

def update_logfile_github(file, existing_file, new_data, fieldnames, url, filename):
    print('prepare updated logfile for github')
    old_data = StringIO(file)
    reader = csv.reader(old_data, fieldnames, lineterminator='\n')
    
    new_file = StringIO()
    writer = csv.writer(new_file, fieldnames, lineterminator='\n')


    for row in reader:
        writer.writerow(row)
 
    writer.writerow(new_data)    
    
    csv_text = StringIO.getvalue(new_file)
    commit_file_github(url, 'master', csv_text, existing_file, 'Automated commit {}'.format(arrow.now().format('YYYY-MM-DD HH:mm:ss')))
    
 
 
 
def commit_file_github(url, branch, content, existing_file, message, auth=GITHUB_AUTH):
    print('commit logfile to github')
    
    encoded_content = base64.b64encode(content)

    headers = {'Cache-Control': 'no-store'}
 
    data = {
        'content': encoded_content,
        'branch': branch,
        'message': message
    }
    
    if existing_file:
        data['sha'] = existing_file

    res = requests.put(url, headers=headers, json=data, auth=auth)
    res.raise_for_status()
 
    return res.json()
 
 
 
def create_logfile(fieldnames, new_data):
    print('create new logfile')
    file = StringIO()
    writer = csv.writer(file, fieldnames, lineterminator='\n')
    writer.writerow(fieldnames)
    writer.writerow(new_data)
    return file
 
 
 
def create_logfile_github(new_data, fieldnames, url, filename):
    print('prepare new github logfile')
    fh = create_logfile(fieldnames, new_data)
    csv_text = StringIO.getvalue(fh)
    commit_file_github(url, 'master', csv_text, None, 'Automated commit {}'.format(arrow.now().format('YYYY-MM-DD HH:mm:ss')))
 

 
def fetch_logfile(date, url):
    print('fetching github data')
 
    try:
        headers = {'Cache-Control': 'no-store'}
        res = requests.get(url, headers=headers)
    
    except requests.exceptions.HTTPError as e:
        raise e
    return res
 
 
 
def update_github_repo(date, new_log, fieldnames, repo_url, data_url, filename):

    request_url = repo_url + filename
    print(request_url)

    try:
        response = fetch_logfile(date, request_url)
        
        if 'Not Found' in response.text:
            print('create new file')
            create_logfile_github(new_log, fieldnames, request_url, filename)
 
        else:
            print('update existing file')
            old_file = response.json()['sha']
            old_data = fetch_logfile_data(data_url, filename)
            update_logfile_github(old_data, old_file, new_log, fieldnames, request_url, filename)
            
    except Exception as e:
        print('Failed to commit data for {}'.format(date))
        print(e)
        raise e
