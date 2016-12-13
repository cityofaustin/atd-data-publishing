import requests
import json
import base64
import pdb


def CreateAuthTuple(credentials_dict):
    return ( credentials_dict['user'], credentials_dict['token'] )
 


def Url_For_Path(repo_url, path):
    return '{}/{}'.format(repo_url, path.strip('/'))



def GetFile(repo_url, path, branch, auth):
    url = Url_For_Path(repo_url, path)
    
    params = {
        'ref': branch,
    }

    
    res = requests.get(url, params=params, auth=auth)
    
    print(res.text)

    res.raise_for_status()

    return res.json()



def CommitFile(url, path, branch, content, message, sha, auth, **options):
    print('commit file to github')

    url = Url_For_Path(url, path)

    #  http://stackoverflow.com/questions/37225035/serialize-in-json-a-base64-encoded-data
    encoded_content = base64.b64encode( content.getvalue().encode('utf-8') )
    encoded_content = encoded_content.decode('utf-8')

    if 'existing_file' in options:
        if encoded_content == options['existing_file']['content']:
             print('Not updating {}, content is identical'.format(path))
             return options['existing_file']
             
    data = {
        'content': encoded_content,
        'branch': branch,
        'message': message,
        'sha' : sha,
        'path' : path
    }

    res = requests.put(url, json=data, auth=auth)
    pdb.set_trace()
    res.raise_for_status()
 
    return res.json()
 
