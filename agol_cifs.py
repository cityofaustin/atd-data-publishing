import pdb
import json
import arrow
import agol_helpers
from secrets import AGOL_CREDENTIALS as creds
'''
TODO:
- create incidents
- are AGOL datetime naive?
'''
now = arrow.now()
now_mills = arrow.now().timestamp * 1000
service_url = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/ATD_road_closures_incidents/FeatureServer/0/'

query_params = {
    'f' : 'json',
    'where' :'EVENT_START_DATETIME IS NOT NULL AND EVENT_END_DATETIME IS NOT NULL',
    'time': now_mills,
    'outFields'  : '*',
    'returnGeometry':True
}

reference_id = '1501'  # arbitrary org reference
reference_name = 'City of Austin' 

fieldmap = {
    'GlobalID' : {
        'type' : 'string',
        'cifs_name' : 'incident',
        'required' : True,
        'note' : 'as id valiue on incident tag'
    },
    'PRIMARY_STREET' : {
        'type' : 'string',
        'cifs_name' : 'street',
        'required' : True
    },
    'EVENT_TYPE' : {
        'type' : 'string',
        'cifs_name' : 'type',
        'required' : True
    },
    'DIRECTIONS_AFFECTED' : {
        'type' : 'string',
        'cifs_name' : 'direction',
        'required' : True
    },
    'DESCRIPTION' : {
        'type' : 'string',
        'cifs_name' : 'description',
        'required' : True
    },
    'EVENT_START_DATETIME' : {
        'type' : 'datetime',
        'cifs_name' : 'startime',
        'required' : True
    },
    'EVENT_END_DATETIME' : {
        'type' : 'datetime',
        'cifs_name' : 'endtime',
        'required' : True
    },
    'CreationDate' : {
        'type' : 'datetime',
        'cifs_name' : 'creationtime',
        'required' : True
    },
    'EditDate' : {
        'type' : 'datetime',
        'cifs_name' : 'updatetime',
        'required' : True
    }
}

def convertToIso(mills):
    return arrow.get(mills/1000).format()


def mapfields(feature, fieldmap):
    new_feature = {}
    for f in feature:
        if f in fieldmap:
            if fieldmap[f]['type'] == 'datetime':
                new_feature[fieldmap[f]['cifs_name']] = convertToIso(feature[f])
            else:
                new_feature[fieldmap[f]['cifs_name']] = feature[f]
    return new_feature
    


def buildPolyline(feature):
    paths = feature['geometry']['paths']
    
    if len(paths) > 1:
        raise Exception #  more than one line path may be bad - need to examine geom of multipoint

    polyline = []
    
    for path in paths:
        for point in path:
            for coord in point:
                print(coord)
                polyline.append(coord)

    return polyline


def main():

    query_params['token'] = agol_helpers.get_token(creds)
    data = agol_helpers.query_layer(service_url, query_params)

    for feature in data['features']:
        incident = mapfields(feature['attributes'], fieldmap)
        incident['polyline'] = buildPolyline(feature)
        #  now add "missing" fields and nest fields as needed
        pdb.set_trace()
        


    pdb.set_trace()

main()



# def build_incident(feature, fieldmap):
    
    
#     "incident": {
#       "-id": "",
#       "creationtime": "",
#       "updatetime": "",
#       "type": "",
#       "description": "",
#       "location": {
#         "street": "",
#         "polyline": "" }
#       "starttime": "",
#       "endtime": "",
#     }
#  