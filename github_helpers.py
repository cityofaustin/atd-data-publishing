import requests
import json
import base64
import pdb


def CreateAuthTuple(credentials_dict):
    return ( credentials_dict['user'], credentials_dict['token'] )
 


def CommitFile(url, branch, content, message, auth):
    print('commit logfile to github')
    
    encoded_content = base64.b64encode(content)

    headers = {'Cache-Control': 'no-store'}
 
    data = {
        'content': encoded_content,
        'branch': branch,
        'message': message
    }
    
    res = requests.put(url, headers=headers, json=data, auth=auth)
    res.raise_for_status()
 
    return res.json()
 
