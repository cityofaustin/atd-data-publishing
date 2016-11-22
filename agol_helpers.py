import requests
import json



def GetToken(creds):
    print("Generate token")
    url = 'https://austin.maps.arcgis.com/sharing/rest/generateToken'
    params = {'username' : creds['username'],'password' : creds['password'], 'referer' : 'http://www.arcgis.com','f' : 'pjson' }
    res = requests.post(url, params=params)
    res = res.json()
    token = res['token']
    return token



def QueryFeatures(url, token):
    url = url + 'query'
    where = 'OBJECTID>0'
    params = {'f' : 'json','where': where , 'outFields'  : '*','token' : token, 'returnGeometry':False }
    res = requests.post(url, params=params)
    res = res.json()
    return res



def AddFeatures(url, token, payload):
    print('add new features to ArcGIS Online feature service')
    url = url + 'addFeatures'
    success = 0
    fail = 0
    params = { 'f':'json','features': json.dumps(payload) ,'token':token}
    res = requests.post(url, data=params)
    res = res.json()

    if 'addResults' not in res: 
        print('FAIL!')
    else:
        print('{} features added'.format(len(res['addResults'])))
    return res



def DeleteFeatures(url, token):
    print('delete all existing ArcGIS Online features')
    url = url + 'deleteFeatures'
    where = 'OBJECTID>0'
    params = {'f' : 'json','where': where , 'outFields'  : '*','token' : token, 'returnGeometry':False }
    res = requests.post(url, params=params)
    res = res.json()
    success = 0
    fail = 0

    for feature in res['deleteResults']:
        if feature['success'] == True:
            success += 1

        else:
            fail += 1
    
    print('{} features deleted and {} features failed to delete'.format( success, fail ))

    return res



def BuildPayload(data):
    print('build data payload')
    
    payload = []

    count = 0

    for record in data:
        new_record = { 'attributes': {}, 'geometry': { 'spatialReference': {'wkid': 4326} } }

        for attribute in record:
            if attribute == 'LATITUDE':
                    new_record['geometry']['y'] = record[attribute]

            elif attribute == 'LONGITUDE':
                    new_record['geometry']['x'] = record[attribute]

            else:
                new_record['attributes'][attribute] = record[attribute]
               
        payload.append(new_record)
    
    return payload