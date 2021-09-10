import datetime as dt
from elasticsearch import Elasticsearch, helpers
from elasticsearch_dsl import Search
import csv
import os
import argparse
import pprint
import pandas as pd


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--username', dest='username', type=str, default=None, required=True)
    parser.add_argument(
        '--password', dest='password', type=str, default=None, required=True)
    args = parser.parse_args()

    es = Elasticsearch([f'http://{args.username}:{args.password}@localhost:19200'])

    print(es.info())

    query = {
            "size": 100000,
            "_source": ["datasetname","ds_replicas_number","attempt_total_duration",
                        "ds_bytes","jeditaskid","attempt_ready_run_delay",
                        "attempt_ready_delta","site_name","site_cloud","jobs_primary_input_fsize",
                        "jobs_timetostart","jobs_total_duration","number_of_jobs","ds_bytes",
                        "attempt_start","jobs_total_inputfilebytes","jobs_total_outputfilebytes",
                        "jobs_timetostart","jobs_total_duration"],
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"ds_data_type": "DAOD"}},
                            {"match": {"ds_data_type_desc": "SUSY1"}},
                            {"match": {"ds_prod_step": "deriv"}},
                            {"match": {"ds_project": "mc16_13TeV"}},
                            {"match": {"task_attemptnr": "0"}},
                            {"match": {"jobstatus": "finished"}},
                            {"match": {"attempt_status": "done"}},
                        ]
                    }
                }
        }

    index = "v2-data-popularity-*"

    print("Start scan")

    df = pd.DataFrame([hit['_source'] for hit in helpers.scan(es, scroll='2m', query=query, index=index, size=1000)])
    print(df.shape)
    df.to_csv('mc16_13TeV.DAOD.deriv.SUSY1.csv')
    print("finished")





if __name__ == "__main__":
    main()