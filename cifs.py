import pdb
import logging
import json
import arrow
import agol_helpers
from secrets import AGOL_CREDENTIALS as creds
'''
TODO:
- upload to socrata
'''
log_directory = '.'

now = arrow.now()
now_s = now.format('YYYY_MM_DD')
now_mills = now.timestamp * 1000

logfile = '{}/cifs_{}.log'.format(log_directory, now_s)
logging.basicConfig(filename=logfile, level=logging.INFO)
logging.info('START AT {}'.format(now.format()))

service_url = 'http://services.arcgis.com/0L95CJ0VTaxqcmED/arcgis/rest/services/ATD_road_closures_incidents/FeatureServer/0/'

query_params = {
    'f' : 'json',
    'where' : 'EVENT_START_DATETIME IS NOT NULL AND EVENT_END_DATETIME IS NOT NULL',
    'time' : now_mills,
    'outFields' : '*',
    'returnGeometry' : True
}

reference_id = '1501'  # arbitrary org reference
reference_name = 'City of Austin' 

fieldmap = {
    'GlobalID' : {
        'type' : 'string',
        'cifs_name' : '-id',
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

incidents = [] 

def convertToIso(mills):
    return arrow.get(mills/1000).format("YYYY-MM-DDTHH:mm:ssZZ") #  iso-8601


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

    return ' '.join(str(coord) for coord in polyline)


def main():
    #  get closure data
    query_params['token'] = agol_helpers.get_token(creds)
    data = agol_helpers.query_layer(service_url, query_params)
    
    logging.info(str(len( data['features'] )))
    
    #  map closure data to CIFS
    for feature in data['features']:
        incident = mapfields(feature['attributes'], fieldmap)
        incident['polyline'] = buildPolyline(feature)
        location = { 
            'street' : incident.pop('street'),
            'polyline' : incident.pop('polyline')
        }
        incident['location'] = location
        #  now add 'missing' fields and nest fields as needed
        incidents.append(incident)
    
    #  write to json
    with open('traffic-incidents.json', 'w') as outfile:
        json.dump(incidents, outfile)

main()



# def build_incident(feature, fieldmap):
    
