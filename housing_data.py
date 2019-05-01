import urllib
import urllib2
import requests
from collections import defaultdict
import csv

original_source_url = ('http://gis.cityofmoorhead.com/arcgis/rest/services/Parcel_Base_Public/MapServer/4/query?f=json&'
                       'returnIdsOnly=true&where=&returnGeometry=false&spatialRel=esriSpatialRelIntersects&geometry=%7B'
                       '%22xmin%22%3A471583.19757073204%2C%22ymin%22%3A163231.15750863933%2C%22xmax%22%3A523840.1420151'
                       '764%2C%22ymax%22%3A205939.49084197267%2C%22spatialReference%22%3A%7B%22wkid%22%3A103701%2C%22la'
                       'testWkid%22%3A103701%7D%7D&geometryType=esriGeometryEnvelope&inSR=103701&outSR=103701')
first_result_url = ('http://gis.cityofmoorhead.com/arcgis/rest/services/Parcel_Base_Public/MapServer/4/query?f=json&'
                    'where=(1%3D1)%20AND%20(%201548909717420%3D1548909717420%20)&returnGeometry=true&spatialRel=esriSpa'
                    'tialRelIntersects&objectIds=21318788%2C21318789%2C21318790%2C21318791%2C21318792%2C21318793%2C2131'
                    '8794%2C21318795%2C21318796%2C21318797%2C21318798%2C21318799%2C21318800%2C21318801%2C21318802%2C213'
                    '18803%2C21318804%2C21318805%2C21318806%2C21318807%2C21318808%2C21318809%2C21318810%2C21318811%2C21'
                    '318812&outFields=*&outSR=103701')
url, query_params = first_result_url.split('?')
params_dict = dict(urllib2.urlparse.parse_qsl(query_params))
del params_dict['objectIds']
base_query = '{}?{}'.format(url, urllib.urlencode(params_dict))
rr = requests.get(original_source_url)
jdata = rr.json()
data_ids = jdata['objectIds']
batch_size = 50
total_batches = len(data_ids) / batch_size
keys = []
row_template = []
key_map = {}
all_data = [keys]
for i in xrange(total_batches + 1):
    batch = data_ids[i * 50: (i + 1) * 50]
    obj_ids_str = '&objectIds={}'.format(','.join((str(b) for b in batch)))
    rr = requests.get('{}{}'.format(base_query, obj_ids_str))
    data = rr.json()
    if i == 0:
        keys = data['features'][0]['attributes'].keys()
        for nn, key in enumerate(keys):
            key_map[key] = nn
            row_template.append('')
    for result in data['features']:
        new_row = row_template[:]
        for key, value in result['attributes'].iteritems():
            new_row[key_map[key]] = value
        all_data.append(new_row)
    print '{} of {} done'.format(i, total_batches + 1)
all_data[0] = keys
with open("/Users/cbabiak/Desktop/houses.txt", "wb") as openf:
    writer = csv.writer(openf, delimiter='\t')
    writer.writerows(all_data)
