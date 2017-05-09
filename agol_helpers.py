import json
import pdb
import logging

import arrow
import requests



logger = logging.getLogger(__name__)



def get_token(creds):
    print("Generate token")
    url = 'https://austin.maps.arcgis.com/sharing/rest/generateToken'
    params = {'username' : creds['user'],'password' : creds['password'], 'referer' : 'http://www.arcgis.com','f' : 'pjson' }
    res = requests.post(url, params=params)
    res = res.json()
    token = res['token']
    return token



def query_all_features(url, token):
    url = url + 'query'
    where = 'OBJECTID>0'
    params = {'f' : 'json','where': where , 'outFields'  : '*','token' : token, 'returnGeometry':False }
    res = requests.post(url, params=params)
    res = res.json()
    return res



def add_features(url, token, payload):
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



def delete_features(url, token):
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



def build_payload(data, **options):
    #  assemble an ArcREST feature object dictionary
    #  spec: http://resources.arcgis.com/en/help/arcgis-rest-api/#/Feature_object/02r3000000n8000000/
    #  records without 'LATITUDE' field are ignored
    print('build data payload')
    
    if 'require_locations' not in options:
        options['require_locations'] = False

    payload = []

    count = 0

    for record in data:
        new_record = { 'attributes': {}, 'geometry': { 'spatialReference': {'wkid': 4326} } }

        if options['require_locations']:

            if not 'LATITUDE' in record:
                continue

        for attribute in record:
            
            if attribute == 'LATITUDE':
                    new_record['geometry']['y'] = record[attribute]

            elif attribute == 'LONGITUDE':
                    new_record['geometry']['x'] = record[attribute]

            new_record['attributes'][attribute] = record[attribute]
               
        payload.append(new_record)
    
    return payload



def parse_attributes(query_results):
    print('parse feature attributes')
    results = []

    for record in query_results['features']:
        results.append(record['attributes'])

    return results



def query_atx_street(segment_id):
    print('query atx street segment {}'.format(segment_id))

    url = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/TRANSPORTATION_street_segment/FeatureServer/0/query'
    
    where = 'SEGMENT_ID={}'.format(segment_id)
    
    params = {'f' : 'json','where': where , 'returnGeometry':False, 'outFields'  : '*'}
    
    res = requests.post(url, params=params)
    
    res = res.json()
    
    if 'features' in res:
        if len(res['features']) > 0:
            return res['features'][0]['attributes']

    else:
        return None



def point_in_poly(service_name, layer_id, point_geom, outfields):
    #  check if point is within polygon feature
    #  return attributes of containing feature 
    #  assume input geometry spatial reference is WGS84
    print('point in poly: {}'.format(service_name))
    point = '{},{}'.format(point_geom[0],point_geom[1]) 
    
    outfields = ','.join( str(e) for e in outfields )

    query_url = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/{}/FeatureServer/{}/query'.format(service_name, layer_id)
    
    params = {'f' : 'json','outFields'  : outfields, 'geometry': point,'returnGeometry':False, 'spatialRel' :'esriSpatialRelIntersects', 'inSR' : 4326, 'geometryType' : 'esriGeometryPoint'}
    
    res = requests.get(query_url, params=params)

    res = res.json()

    if 'features' in res:
        if len(res['features']) > 0:
            return res['features'][0]['attributes']

        else:
            return ''

    else:
        raise ValueError('point in poly request failure')



def parse_response(res_msg, req_type):
    print('parse response')
    success = 0
    fail = 0

    for record in res_msg[ '{}Results'.format(req_type) ]:
        if 'success' in record:
            success += 1
        else:
            fail += 1

    return {
        "success" : success,
        "fail" : fail
    }