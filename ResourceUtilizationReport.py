import pandas as pd
import argparse
from elasticsearch import Elasticsearch, helpers
import datetime
import numpy as np

index = "v2-data-popularity-*"

grouping_parameters = {
    'ds_scope': {
        'synonym': 'Project',
    }
}

level='site_name'
split_by='site_cloud'
weight = 'number_of_jobs'


aggregated_parameters = {
    'Total Number of Jobs': {
        'aggfunc': 'sum',
        'aggfunc_pivot': np.sum,
        'synonym': 'number_of_jobs',
        'default': None,
    },
    'Jobs Waiting Time (minutes) per Task': {
        'aggfunc': 'wavg',
        'aggfunc_pivot': np.mean,
        'synonym': 'jobs_timetostart',
        'default': None,
        'weight': 'number_of_jobs',
        'inner_group': 'jeditaskid',
    },
    'Jobs Execution Time (minutes) per Task': {
        'aggfunc': 'wavg',
        'aggfunc_pivot': np.mean,
        'synonym': 'jobs_total_duration',
        'default': None,
        'weight': 'number_of_jobs',
        'inner_group': 'jeditaskid',
    },
    'Individual Analysis Users': {
        'aggfunc': 'nunique',
        'aggfunc_pivot': np.max,
        'synonym': 'username',
        'default': None,
    },
    'Jobs Walltime (minutes) per Task': {
        'aggfunc': 'wavg',
        'aggfunc_pivot': np.mean,
        'synonym': 'jobs_total_walltime',
        'default': None,
        'weight': 'number_of_jobs',
        'inner_group': 'jeditaskid',
    },
    'Jobs CPU Time (minutes) per Task': {
        'aggfunc': 'wavg',
        'aggfunc_pivot': np.mean,
        'synonym': 'jobs_cpuconsumptiontime',
        'default': None,
        'weight': 'number_of_jobs',
        'inner_group': 'jeditaskid',
    },
    'Fraction of Task Attempt Execution Time': {
        'aggfunc': 'wavg',
        'aggfunc_pivot': np.mean,
        'synonym': 'fraction_of_attempt_duration',
        'default': None,
        'weight': 'number_of_jobs',
        'inner_group': 'jeditaskid',
    },
    'Total Input Data (TB)': {
        'aggfunc': 'sum',
        'aggfunc_pivot': np.sum,
        'synonym': 'jobs_total_inputfilebytes',
        'default': None,
    },
    'Total Output Data (TB)': {
        'aggfunc': 'sum',
        'aggfunc_pivot': np.sum,
        'synonym': 'jobs_total_outputfilebytes',
        'default': None,
    },
    'Max Input Volume per Task (TB)': {
        'aggfunc': 'max',
        'aggfunc_pivot': np.max,
        'synonym': 'jobs_total_inputfilebytes',
        'default': None,
    },
}


last_week = datetime.date.today() - datetime.timedelta(days=7)
last_month = datetime.date.today() - datetime.timedelta(days=30)
last_2_months = datetime.date.today() - datetime.timedelta(days=60)
date_intervals = {'last_week': last_week,
                'last_month': last_month,
                'last_2_months': last_2_months}


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


def to_pivot(df, group_by, level, agg_target, meta=None):
    return df.pivot_table(index=group_by, columns=level, values=agg_target, aggfunc=meta['aggfunc_pivot']) \
        if meta is not None else df.pivot_table(index=group_by, columns=level, values=agg_target, aggfunc='first')


def wavg(group, avg_name, weight_name):
    """ http://stackoverflow.com/questions/10951341/pandas-dataframe-aggregate-function-using-multiple-columns
    In rare instance, we may not have weights, so just return the mean. Customize this if your business case
    should return otherwise.
    """
    d = group[avg_name]
    w = group[weight_name]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return d.mean()


def queues_aggregation(df,
                       group_by,
                       agg_target,
                       meta,
                       pivot_split,
                       splitted_pivots,
                       split_by=None,
                       level='site_name',
                       weights='number_of_jobs'):
    print(f'First regroup by {list(group_by.keys())} and {level} for {agg_target}')
    by = list(group_by.keys())
    group = [meta['inner_group']] + by + [split_by, level] if meta.get('inner_group',None) else by + [split_by, level]
    if meta['aggfunc'] == 'mean':
        res = df.groupby(group)[meta['synonym']].mean().reset_index()
    elif meta['aggfunc'] == 'sum':
        res = df.groupby(group)[meta['synonym']].sum().reset_index()
    elif meta['aggfunc'] == 'median':
        res = df.groupby(group)[meta['synonym']].median().reset_index()
    elif meta['aggfunc'] == 'wavg':
        res = df.groupby(group + [meta['synonym']])[weights].sum().reset_index()
        res.drop(meta['inner_group'],axis=1,inplace=True)
        group.remove(meta['inner_group'])
        res = res.groupby(group)[meta['synonym'], meta['weight']].apply(
            lambda x: wavg(x, meta['synonym'], meta['weight'])).reset_index(name=meta['synonym'])
    elif meta['aggfunc'] == ' q90':
        res = df.groupby(group)[meta['synonym']].quantile(0.9).reset_index()
    elif meta['aggfunc'] == 'nunique':
        res = df.groupby(group)[meta['synonym']].nunique().reset_index()
    elif meta['aggfunc'] == 'max':
        res = df.groupby(group)[meta['synonym']].max().reset_index()
    print(f'Number of groups for {agg_target}:')
    print(res.shape[0])
    print(f'Second regroup by {group_by.keys()} for {level} and {agg_target}')

    if split_by is not None:
        split_df = res.groupby(split_by)
        for name, gr in split_df:
            splitted_pivots[name][agg_target] = to_pivot(gr, by, level, meta['synonym']).round(decimals=1)

        # pivot_total[target] = to_pivot(res, by, level, target).round(decimals=3)
        # pivot_total[target] = pivot_total[target].sort_values(by=list(pivot_total[target].columns)[0], ascending=False)

        pivot_split[agg_target] = to_pivot(res, by, split_by, meta['synonym'], meta).round(decimals=1)


def multiple_list_dfs(df_list, sheets, spaces, writer):
    row = 0
    for dataframe in df_list:
        dataframe.style.background_gradient(cmap='Blues').to_excel(writer,sheet_name=sheets,startrow=row, startcol=0)
        row = row + len(dataframe.index) + spaces + 1


def multiple_dict_dfs(df_dict, sheets, spaces, writer):
    row = 0
    for k,v in df_dict.items():
        v.style.background_gradient(cmap='Blues').to_excel(writer,sheet_name=sheets,startrow=row+1, startcol=0)
        worksheet = writer.sheets[sheets]
        worksheet.write_string(row, 0, k)
        row = row + len(v.index) + spaces + 1


def build_popularity_report(df):
    columns = list(df[split_by].unique())

    # initialize empty dictionaries for each cloud
    splitted_pivots = {}
    for i in columns:
        splitted_pivots[i] = {k: v['default'] for k, v in aggregated_parameters.items()}

    # initialize empty dictionary for clouds
    pivot_split = {k: v['default'] for k, v in aggregated_parameters.items()}

    # generating dataframes for each group
    for name, meta in aggregated_parameters.items():
        queues_aggregation(df,
                           grouping_parameters,
                           name,
                           meta,
                           pivot_split,
                           splitted_pivots,
                           split_by,
                           level,
                           weight)

    print('Finished:')
    return splitted_pivots, pivot_split

def save_to_file(outputfile, splitted_pivots, pivot_split):
    writer = pd.ExcelWriter(outputfile, engine='xlsxwriter')
    multiple_dict_dfs(pivot_split, split_by, 2, writer)
    # multiple_dict_dfs(pivot_total, level, 2, writer)

    for k, v in splitted_pivots.items():
        multiple_dict_dfs(v, k, 2, writer)

    try:
        writer.save()
        print(f'Save to file {outputfile}')
    except Exception as ex:
        print(ex)


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
            reports[k] = build_popularity_report(get_data(es, v))
            print(f'Report {k} finished!')

        for k,v in reports.items():
            today = datetime.date.today().strftime('%Y-%m-%d')
            outputfile = f'resource_utilization_{today}_{k}.xlsx'
            save_to_file(outputfile, v[0], v[1])

    except Exception as e:
        print(e)


if __name__ == "__main__":
    main()