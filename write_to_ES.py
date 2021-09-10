import datetime as dt
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
import os
import argparse
import pprint
import pandas as pd
from collections import OrderedDict
import numpy as np
import ast
import hashlib

import csv
import json

def main():

    # parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     '--username', dest='username', type=str, default=None, required=True)
    # parser.add_argument(
    #     '--password', dest='password', type=str, default=None, required=True)
    # args = parser.parse_args()

    es = Elasticsearch(['http://localhost:9200'])

    print(es.info())

    # create index if not exists
    with open("mapping.json", "r", encoding="utf-8") as file:
        es_mapping = json.loads(file.read())
        if not es.indices.exists(index='bulk-test'):
            es.indices.create(index='bulk-test', ignore=400, body=es_mapping)
            print('Index created!')

    # date_cols = ['task_creationdate',
    #             'task_start_time',
    #             'task_end_time',
    #             'task_modificationtime',
    #             'jobs_creationtime',
    #             'jobs_starttime',
    #             'jobs_endtime',
    #             'attempt_start',
    #             'attempt_finished',
    #             'attempt_defined_first',
    #             'attempt_ready_first',
    #             'attempt_scouting_first',
    #             'attempt_running_first',
    #             'ds_deleted_at',
    #             'ds_created_at',
    #             'ds_closed_at']

    # "settings": {
    #     "index.mapping.ignore_malformed": false,
    #     "index.mapping.coerce": false
    # },

    df = pd.read_csv('output_1hour.csv')

    try:
        df['ds_replicas_info'] = df['ds_replicas_info'].apply(ast.literal_eval)
    except Exception as ex:
        df['ds_replicas_info'] = None

    df.fillna(np.nan,inplace=True)
    df = df.replace({np.nan: None})

    dict_records = df.to_dict('records')
    # x = []
    # for doc in dict_records:
    #     x.append(json.dumps(doc))
    #
    # helpers.bulk(es, x, index="bulk-test", doc_type="_doc", raise_on_error=True, request_timeout=200)

    import requests
    headers = {'Content-type': 'application/json',
               'Accept': 'text/plain'}
    jsons = []
    for doc in dict_records:
        _id = hash(str(doc['jeditaskid'])+str(doc['datasetname'])+str(doc['task_modificationtime'])+str(doc['task_attemptnr'])+str(doc['queue'])+str(doc['jobstatus']))
        jsons.append('{"index": {"_index": "bulk-test", "_id": "%s"}}\n%s\n' % (_id, json.dumps(doc)))

    data = ''.join(jsons)
    print(data)
    response = requests.post("http://localhost:9200/_bulk", data=data, headers=headers)
    print(response)

    # print(dict_records)

    # get all unique jeditaskid from input data
    tasks = list(set([str(row['jeditaskid']) for row in dict_records]))
    n_tasks = len(tasks)
    tasks = ', '.join(tasks)
    # remove data about these tasks from existing ES index
    remove_query = {
          "query": {
            "constant_score" : {
               "filter" : {
                  "terms" : {
                     "jeditaskid" : [tasks]
                  }
               }
             }
          }
        }

    remove_query = str(remove_query).replace("['","[").replace("']","]").replace("'",'"')
    try:
        res = es.delete_by_query(index="updated-test-index", body=remove_query)
        print(res)
        print(f'{n_tasks} were deleted from ES!')
    except Exception as e:
        print(e)


    #write to ES
    print("Write to ES")
    def gendata():
        for doc in dict_records:
            # print(json.dumps(doc))
            yield {
                "_index": "updated-test-index",
                "_source": json.dumps(doc)
            }

    for success, info in helpers.parallel_bulk(es, gendata(), thread_count=4, chunk_size=500, queue_size=4):
        if not success:
            raise RuntimeError(f"Error indexando query a ES: {info}")



if __name__ == "__main__":
    main()