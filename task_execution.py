import datetime as dt
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
import csv
import os
import argparse
import pprint
import pandas as pd
import plotly.graph_objects as go
import urllib, json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--username', dest='username', type=str, default=None, required=True)
    parser.add_argument(
        '--password', dest='password', type=str, default=None, required=True)
    parser.add_argument(
        '--ds_name', dest='ds_name', type=str, default=None, required=True)
    args = parser.parse_args()

    es = Elasticsearch([f'http://{args.username}:{args.password}@localhost:19200'])
    print(es.info())
    query = {
          "size": 1000,
          "_source": ["jeditaskid",
                      "institute_name",
                      "task_status",
                      "ds_replicas_sites",
                      # "ds_bytes",
                      "site_name",
                      # "task_attemptnr",
                      # "attempt_total_duration",
                      # "attempt_status",
                      "jobstatus",
                      "number_of_jobs",
                      # "jobs_primary_input_fsize",
                      # "jobs_total_duration",
                      # "jobs_timetostart"
                      ],
          "query": {
            "bool": {
              "must": [
                  {
                    "range": {
                        "task_modificationtime": {
                            "gte": "2021-03-15 00:00:00",
                            "lte": "2021-04-15 00:00:00"
                        }
                    }
                  },
                {
                  "term": {
                     "datasetname.keyword": args.ds_name
                  }
                },
                {
                  "match_all": {}
                }
                ]
            }
          }
        }

    print(query)
    index = "v2-data-popularity-*"
    res = es.search(index=index, body=query)
    print(res)
    df = pd.DataFrame([hit['_source'] for hit in res['hits']['hits']])
    # df['processed_data%'] = (df['jobs_primary_input_fsize']/df['ds_bytes'])*100
    grouped = df.groupby(['jeditaskid',
                          'institute_name',
                          'task_status',
                          # 'task_attemptnr',
                          # 'attempt_status',
                          # 'attempt_total_duration',
                          'ds_replicas_sites',
                          'site_name',
                          'jobstatus'])['number_of_jobs'].sum()
    grouped = grouped.reset_index()

    grouped.to_csv('mc16_13TeV:mc16_13TeV.364196.Sherpa_221_NNPDF30NNLO_Wtaunu_MAXHTPTV500_1000.deriv.DAOD_SUSY5.e5340_s3126_r9364_r9315_p4172_tid21571048_00.csv')

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            label=["A1", "A2", "B1", "B2", "C1", "C2"],
            color="blue"
        ),
        link=dict(
            source=[0, 1, 0, 2, 3, 3],  # indices correspond to labels, eg A1, A2, A1, B1, ...
            target=[2, 3, 3, 4, 4, 5],
            value=[8, 4, 2, 8, 4, 2]
        ))])

    fig.update_layout(title_text="Basic Sankey Diagram", font_size=10)
    fig.show()

if __name__ == "__main__":
    main()