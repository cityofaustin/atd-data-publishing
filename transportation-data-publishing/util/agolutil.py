'''
Helper methods to work with ArcGIS for Python API, mixed
in with a few manual ArcGIS REST API requests.

#TODO
- use arcgis library api instead of custom functions
'''
import json
import pdb
import urllib

from arcgis.gis import GIS
from arcgis.features import FeatureLayerCollection
import requests

def get_token(auth, base_url='https://austin.maps.arcgis.com'):
    url = f'{base_url}/sharing/rest/generateToken'

    params = {
        'username' : auth['user'],
        'password' : auth['password'],
        'referer' : 'http://www.arcgis.com',
        'f' : 'pjson'
    }

    res = requests.post(url, params=params)
    res.raise_for_status()
    res = res.json()
    return res.get('token')


def query_atx_street(segment_id, token):
    url = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/TRANSPORTATION_street_segment/FeatureServer/0/query'

    where = 'SEGMENT_ID={}'.format(segment_id)

    params = {
        'f' : 'json',
        'where' : where,
        'returnGeometry' : False,
        'outFields'  : '*',
        'token' : token
    }
    
    res = requests.post(url, params=params)
    res.raise_for_status()
    
    return res.json()


def point_in_poly(service_name, layer_id, params):
    '''
    Check if point is within polygon feature and return attributes of containing
    feature. Assume input geometry spatial reference is WGS84.
    docs: http://resources.arcgis.com/en/help/arcgis-rest-api/index.html#//02r3000000p1000000

    #TODO: replace with arcgis library api
    '''
    query_url = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/ArcGIS/rest/services/{}/FeatureServer/{}/query'.format(service_name, layer_id)
    
    if 'spatialRel' not in params:
        params['spatialRel'] = 'esriSpatialRelIntersects'

    res = requests.get(query_url, params=params)
    res.raise_for_status()

    return res.json()


def get_item(auth=None, item_type='layer', layer_id=0, service_id=None):
    if not service_id:
        raise Exception('Service ID is required')

    gis = GIS('http://austin.maps.arcgis.com',
              username=auth['user'],
              password=auth['password'])

    item = gis.content.get(service_id)
    
    if item_type=='layer':
        return item.layers[layer_id]
    
    elif item_type=='table':
        return item.tables[layer_id]
    
    else:
        raise Exception('Unknown item type requested.')


def feature_collection(data,
                       lat_field='latitude',
                       lon_field='longitude',
                       spatial_ref=4326):
    '''
    Assemble an ArcREST featureCollection object
    spec: http://resources.arcgis.com/en/help/arcgis-rest-api/#/Feature_object/02r3000000n8000000/
    
    TODO: Make more 
    '''
    features = []

    for record in data:
        
        new_record = {
            'attributes': {},
            'geometry': {
                'spatialReference': {
                    'wkid': spatial_ref
                }
            }
        }
        
        new_record['attributes'] = record

        if record.get('paths'):
            # Handle polyline geometry
            new_record['geometry']['paths'] = record.pop('paths')

        elif record.get(lat_field) and record.get(lon_field):
            # Handle point geometry
            new_record['geometry']['x'] = record.get(lon_field)
            new_record['geometry']['y'] = record.get(lat_field)

        else:
            #  strip geometry from records with missing/unkown geometry data
            new_record.pop('geometry')

        features.append(new_record)
    
    return features


def handle_response(agol_response, raise_exception=True):
    '''
    Inspect an AGOL API POST response for errors
    '''

    results = {
        'success' : 0,
        'fail' : 0
    }

    for result_type in agol_response.keys():
        if 'Result' not in result_type:
            '''
            Response is a dict with 'sucess' or 'fail' keys
            '''
            if agol_response.get('success'):
                results['success'] = 1
            else:
                results['fail'] = 1
        else:
            '''
            Response is a dict with addResults, deleteResults etc. arrays
            '''
            success = [record for record in agol_response[result_type] if record.setdefault('success', False) ]
            fail = [record for record in agol_response[result_type] if not record.get('success') ]
        
            results['success'] += len(success)
            results['fail'] += len(fail)

        if raise_exception and results['fail'] > 0:

           raise Exception(str(agol_response))

        else:
            return results




