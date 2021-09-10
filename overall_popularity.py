import pandas as pd
import argparse
from elasticsearch import Elasticsearch, helpers
import datetime
import numpy as np

index = "v2-data-popularity-*"

group_by = ['ds_scope', 'ds_data_type_desc']

targets = {
    'jeditaskid': 'nunique',
    'number_of_jobs': 'sum',
    'username': 'nunique',
    'institute_name': 'nunique',
    'institute_country': 'nunique',
    'attempt_total_duration': 'mean',
    'attempt_ready_run_delay': 'mean',
    'attempt_pending_delta': 'mean',
    'attempt_running_delta': 'mean',
    'attempt_ready_delta': 'mean',
    'queue_time': 'mean',
    'site_name': 'nunique',
    'site_cloud': 'nunique',
    'jobs_total_inputfilebytes': 'sum',
    'jobs_total_outputfilebytes': 'sum',
    'datasetname': 'nunique',
    'ds_replicas_number': 'mean'
}

column_names = {
    'jeditaskid': 'n_tasks',
    'number_of_jobs': 'n_jobs',
    'username': 'n_users',
    'institute_name': 'n_institutes',
    'institute_country': 'n_countries',
    'attempt_total_duration': 'attempt_duration(min)',
    'attempt_ready_run_delay': 'attempt_ready_run_delay(min)',
    'attempt_pending_delta': 'attempt_pending(min)',
    'attempt_running_delta': 'attempt_running(min)',
    'queue_time': 'queue_time(min)',
    'site_name': 'n_sites',
    'site_cloud': 'n_clouds',
    'jobs_total_inputfilebytes': 'input_TB',
    'jobs_total_outputfilebytes': 'output_TB',
    'datasetname': 'n_datasets',
    'ds_replicas_number': 'replication_factor'
}


last_week = datetime.date.today() - datetime.timedelta(days=7)
last_month = datetime.date.today() - datetime.timedelta(days=30)
last_2_months = datetime.date.today() - datetime.timedelta(days=60)
date_intervals = {'Last Week': last_week,
                'Last Month': last_month,
                'Last 2 Months': last_2_months}


def get_data(es, start_date):
    query = {
        "size": 1000000,
        "_source": ["jeditaskid",
                    "number_of_jobs",
                    "username",
                    "institute_name",
                    "institute_country",
                    "attempt_total_duration",
                    "attempt_ready_run_delay",
                    "attempt_pending_delta",
                    "attempt_running_delta",
                    "attempt_ready_delta",
                    "site_name",
                    "site_cloud",
                    "jobs_total_inputfilebytes",
                    "jobs_total_outputfilebytes",
                    "datasetname",
                    "ds_replicas_number",
                    "ds_data_type_desc",
                    "ds_data_type",
                    "ds_scope",
                    "campaign",
                    "jobs_primary_input_fsize",
                    "ds_bytes",
                    "jobs_timetostart",
                    "jobs_total_duration",
                    "jobs_total_walltime",
                    "jobs_cpuconsumptiontime",
                    "task_creationdate",
                    "attempt_start"
                    ],
        "query": {
            "bool": {
                "must": [
                    {"match": {"ds_data_type": "DAOD"}},
                    {"match": {"task_attemptnr": "0"}},
                    {"match": {"jobstatus": "finished"}},
                    {"terms": {"attempt_status": ["done", "finished"]}}
                ],
                "filter": [
                    {
                        "range": {
                            "task_modificationtime": {
                                "gte": start_date.strftime('%Y-%m-%d %H:%M:%S'),
                                "lte": datetime.date.today().strftime('%Y-%m-%d %H:%M:%S')
                            }
                        }
                    }
                ]
            }
        }
    }
    try:
        df = pd.DataFrame(
            [hit['_source'] for hit in helpers.scan(es, scroll='2m', query=query, index=index, size=10000, request_timeout=50)])
        print(df.shape)
        parameters_converter(df)
        print('DataFrame created!')
        return df
    except Exception as e:
        print(e)


def build_popularity_report(df, start_date):
    print('Start generating overall popularity report:')
    res = df.groupby(group_by).agg(targets)
    res.rename(columns=column_names, inplace=True)
    res['TimestampStart'] = datetime.date.today().strftime('%Y-%m-%d')
    res['TimestampEnd'] = start_date.strftime('%Y-%m-%d')
    print('Finished:')
    return res


def parameters_converter(df):
    df['attempt_total_duration'] = df['attempt_total_duration'] / 60
    df['jobs_total_inputfilebytes'] = df['jobs_total_inputfilebytes'] / 1099511627776
    df['jobs_total_outputfilebytes'] = df['jobs_total_outputfilebytes'] / 1099511627776
    df['ds_data_type_desc'] = df['ds_data_type_desc'].str.replace('\d+', '')
    df['fraction_of_dataset'] = df['jobs_primary_input_fsize'] / df['ds_bytes']
    df['jobs_timetostart'] = df['jobs_timetostart'] / 60
    df['jobs_total_duration'] = df['jobs_total_duration'] / 60
    df['jobs_total_walltime'] = df['jobs_total_walltime'] / 60
    df['jobs_cpuconsumptiontime'] = df['jobs_cpuconsumptiontime'] / 60
    df['fraction_of_attempt_duration'] = df['jobs_total_duration'] / (
                df['attempt_total_duration'] / 60)
    df[['attempt_start','task_creationdate']] = df[['attempt_start','task_creationdate']].apply(pd.to_datetime)
    df['queue_time'] = (df['task_creationdate'] - df['attempt_start']).dt.seconds / 60


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--username', dest='username', type=str, default=None, required=True)
    parser.add_argument(
        '--password', dest='password', type=str, default=None, required=True)
    parser.add_argument(
        '--from', dest='from_date', type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S'),
        default=datetime.date.today() - datetime.timedelta(days=2))
    parser.add_argument(
        '--to', dest='to_date', type=lambda s: datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S'),
        default=datetime.date.today())
    parser.add_argument(
        '--output', dest='output_path', type=str,
        default=f"Popularity Tables/{datetime.date.today()}.xlsx")
    args = parser.parse_args()

    try:
        es = Elasticsearch([f'http://{args.username}:{args.password}@localhost:19200'])
        print(es.info())

        reports = {}

        for k,v in date_intervals.items():
            reports[k] = build_popularity_report(get_data(es, v), v)
            print(f'Report {k} finished!')

        writer = pd.ExcelWriter(args.output_path, engine='xlsxwriter')

        for k,v in reports.items():
            v = v.fillna(0)
            v.style.background_gradient(cmap='Blues').to_excel(writer, sheet_name=k)
            worksheet = writer.sheets[k]
            print('worksheet finished!')

        try:
            writer.save()
            print(f'Save to file {args.output_path}')
        except Exception as ex:
            print(ex)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()