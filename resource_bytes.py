from json import loads

from requests import post
from datetime import datetime
import pandas as pd

headers = {}

headers[
    'Authorization'] = 'Bearer eyJrIjoiTWg1NDNqUXFFVDhBMVNYMGhVRGVhSThYZkNDbFdkQ0YiLCJuIjoiVGFpd2FuX0RETSIsImlkIjoxN30='
headers['Content-Type'] = 'application/json'
headers['Accept'] = 'application/json'

base = "https://monit-grafana.cern.ch"
url = "api/datasources/proxy/9037/_msearch?max_concurrent_shard_requests=256"

date_from_str = '01.03.2021 00:00:00'
date_to_str = '02.03.2021 00:00:00'

date_from = datetime.strptime(date_from_str, '%d.%m.%Y %H:%M:%S')
date_to = datetime.strptime(date_to_str, '%d.%m.%Y %H:%M:%S')

date_from_ms = str(int(date_from.timestamp() * 1000))
date_to_ms = str(int(date_to.timestamp() * 1000))

# query = """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_rucioacc_enr_site*"]}\n{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":""" + date_from_ms + ""","lte":""" + date_to_ms + ""","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"data.account:* AND data.campaign:* AND data.country:* AND data.cloud:* AND data.datatype:* AND data.datatype_grouped:* AND data.prod_step:* AND data.provenance:* AND data.rse:* AND data.scope:* AND data.experiment_site:* AND data.stream_name:* AND data.tier:* AND data.token:* AND data.tombstone:* AND NOT(data.tombstone:UNKNOWN) AND data.rse:/.*().*/ AND NOT data.rse:/.*(none).*/"}}]}},"aggs":{"4":{"terms":{"field":"data.site","size":500,"order":{"_key":"desc"},"min_doc_count":0},"aggs":{"5":{"terms":{"field":"data.datatype","size":500,"order":{"_key":"desc"},"min_doc_count":0},"aggs":{"6":{"terms":{"field":"data.datatype_grouped","size":500,"order":{"_key":"desc"},"min_doc_count":0},"aggs":{"7":{"terms":{"field":"data.prod_step","size":500,"order":{"_key":"desc"},"min_doc_count":0},"aggs":{"3":{"terms":{"field":"data.scope","size":500,"order":{"_key":"desc"},"min_doc_count":0},"aggs":{"1":{"sum":{"field":"data.bytes"}}}}}}}}}}}}}}\n"""

query = """{"search_type":"query_then_fetch","ignore_unavailable":true,"index":["monit_prod_rucioacc_enr_site*","monit_prod_rucioacc_enr_site*"]}\n{"size":0,"query":{"bool":{"filter":[{"range":{"metadata.timestamp":{"gte":""" + date_from_ms + ""","lte":""" + date_to_ms + ""","format":"epoch_millis"}}},{"query_string":{"analyze_wildcard":true,"query":"data.account:* AND data.campaign:* AND data.country:* AND data.cloud:* AND data.datatype:* AND data.datatype_grouped:* AND data.prod_step:* AND data.provenance:* AND data.rse:* AND data.scope:* AND data.experiment_site:* AND data.stream_name:* AND data.tier:* AND data.token:* AND data.tombstone:* AND NOT(data.tombstone:UNKNOWN) AND data.rse:/.*().*/ AND NOT data.rse:/.*(none).*/"}}]}},"aggs":{"data_site":{"terms":{"field":"data.site","size":500,"order":{"_key":"desc"},"min_doc_count":0},"aggs":{"data_datatype":{"terms":{"field":"data.datatype","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"data_datatype_grouped":{"terms":{"field":"data.datatype_grouped","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"data.prod_step":{"terms":{"field":"data.prod_step","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"data_scope":{"terms":{"field":"data.scope","size":500,"order":{"_key":"desc"},"min_doc_count":1},"aggs":{"data_bytes":{"sum":{"field":"data.bytes"}}}}}}}}}}}}}}\n"""

request_url = "%s/%s" % (base, url)
r = post(request_url, headers=headers, data=query, timeout=99999)

data = []

if r.ok:
    sites = loads(r.text)['responses'][0]['aggregations']['data_site']['buckets']
    for site in sites:
        for datatype in site['data_datatype']['buckets']:
            for datatypeg in datatype['data_datatype_grouped']['buckets']:
                for prodstep in datatypeg['data.prod_step']['buckets']:
                    for data_scope in prodstep['data_scope']['buckets']:
                        bytes = data_scope['data_bytes']['value']
                        data.append({'datetime': date_from,
                                     'site': site['key'],
                                     'datatype': datatype['key'],
                                     'datatypeg': datatypeg['key'],
                                     'prodstep': prodstep['key'],
                                     'scope': data_scope['key'],
                                     'bytes': bytes
                                     })
df = pd.DataFrame(data)
df.to_excel('resource_bytes.xlsx')
print('completed')
