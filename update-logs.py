from StringIO import StringIO
import base64
import csv
import requests
import arrow
from secrets import GITHUB_USERNAME
from secrets import GITHUB_TOKEN

REPO_URL = 'https://api.github.com/repos/cityofaustin/transportation-logs/contents'
DATA_URL = 'https://raw.githubusercontent.com/cityofaustin/transportation-logs/master/'
FIELDNAMES = ['date_time', 'change_detected']
GITHUB_AUTH = (GITHUB_USERNAME, GITHUB_TOKEN)

today = arrow.now('America/Chicago')

filename_short = today.format('YYYY-MM-DD')

filename = 'logs/signals-on-flash/2016-09-06.csv'
#  filename = 'logs/signals-on-flash/{}.csv'.format(date.format('YYYY-MM-DD'))

request_url = REPO_URL + filename

def url_for_path(path):
    return '{}/{}'.format(REPO_URL, path.strip('/'))

def commit_file_github(path, branch, content, message, auth=GITHUB_AUTH):

    url = url_for_path(path)

    encoded_content = base64.b64encode(content)

    data = {
        'content': encoded_content,
        'branch': branch,
        'message': message,
    }

    res = requests.put(url, json=data, auth=auth)

    print(res.text)

    res.raise_for_status()

    return res.json()



def write_permits(file, date, fieldnames):
    writer = csv.writer(file, fieldnames, lineterminator='\n')
    writer.writerow(fieldnames)
    writer.writerow([date.format('YYYY-MM-DD HH:mm:ss'), 'YES'])



def update_logfile_github(date):
    print("UPDATE_LOGFILE")



def create_logfile_github(date):
    fh = StringIO()
    write_permits(fh, date, FIELDNAMES)
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

        if not 'Not Found' in html:
            update_logfile_github(date)

        else:
            create_logfile_github(date)

    except Exception as e:
        print('Failed to commit data for {}'.format(date))
        print(e)
        raise e


log_signal_status_etl(today)


