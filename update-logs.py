from StringIO import StringIO
import arrow
import requests
import csv
import json
import base64
from secrets import GITHUB_USERNAME
from secrets import GITHUB_TOKEN
 
import pdb
 
REPO_URL = 'https://api.github.com/repos/cityofaustin/transportation-logs/contents'
DATA_URL = 'https://raw.githubusercontent.com/cityofaustin/transportation-logs/master/'
FIELDNAMES = ['date_time', 'change_detected']
GITHUB_AUTH = (GITHUB_USERNAME, GITHUB_TOKEN)
 
today = arrow.now('America/Chicago')
 
filename_short = today.format('YYYY-MM-DD')
 
filename = 'logs/signals-on-flash/6666-66-66.csv'
#  filename = 'logs/signals-on-flash/{}.csv'.format(date.format('YYYY-MM-DD'))
 
request_url = REPO_URL + filename
 
def fetch_logfile_data(date):
    res = requests.get(DATA_URL + filename)
    return str(res.text)
 
 
def update_logfile_github(file, date, fieldnames):
    old_file = StringIO(file)
    reader = csv.reader(old_file, fieldnames, lineterminator='\n')
    
    new_file = StringIO()
    writer = csv.writer(file, fieldnames, lineterminator='\n')
 
    for row in reader:
        writer.writerow(row)
 
    writer.writerow([date.format('YYYY-MM-DD HH:mm:ss'), 'YES'])    
    
    csv_text = StringIO.getvalue(new_file)
    commit_file_github(filename, 'master', csv_text, 'Automated commit {}'.format(filename_short))
    
 
 
def url_for_path(path):
    return '{}/{}'.format(REPO_URL, path.strip('/'))
 
 
 
def commit_file_github(path, branch, content, message, auth=GITHUB_AUTH):
 
    print('commit to github')
    url = url_for_path(path)
    encoded_content = base64.b64encode(content)
 
    data = {
        'content': encoded_content,
        'branch': branch,
        'message': message,
    }
 
    res = requests.put(url, json=data, auth=auth)
    res.raise_for_status()
 
    return res.json()
 
 
 
def write_logfile(date, fieldnames):
    file = StringIO()
    writer = csv.writer(file, fieldnames, lineterminator='\n')
    writer.writerow(fieldnames)
    writer.writerow([date.format('YYYY-MM-DD HH:mm:ss'), 'YES'])
    return file
 
 
 
def create_logfile_github(date):
    fh = write_logfile(date, FIELDNAMES)
    csv_text = StringIO.getvalue(fh)
    commit_file_github(filename, 'master', csv_text, 'Automated commit {}'.format(filename_short))
 
 
def fetch_logfile(date):
    print('Getting data for {}'.format(date, date))
 
    try:
        res = requests.get(request_url)
    
    except requests.exceptions.HTTPError as e:
        raise e
    return res.text
 
 
 
def log_signal_status_etl(date):
    try:
        html = fetch_logfile(date)
 
        if 'Not Found' in html:
            print('create new file')
            create_logfile_github(date, old_data)
 
        else:
            print('update existing file')
            old_data = fetch_logfile_data(date)
            update_logfile_github(old_data, date, FIELDNAMES)
            
    except Exception as e:
        print('Failed to commit data for {}'.format(date))
        print(e)
        raise e
 
 
log_signal_status_etl(today)
