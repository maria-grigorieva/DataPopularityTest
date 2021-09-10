import pandas as pd
import numpy as np
import glob
import plotly.express as px
import ast
import xlsxwriter

grouping_parameters = {
    'ds_scope': {
        'synonym': 'Project',
    },
    # 'datasetname': {
    #     'synonym': 'Dataset Name',
    # }
    # 'ds_data_type_desc': {
    #     'synonym': 'Data Type'
    # },
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


def most_popular(df, by):
    """
    Get the most popular datasets for the analysis
    :param df:
    :param by:
    :return:
    """
    popular = df.groupby(by[0])['jeditaskid'].nunique()
    popular = popular.sort_values(ascending=False)
    return popular.index[0:5000]


def filtration(df, by=None, popular=None):
    if popular is not None:
        df = df[df[by[0]].isin(popular)]
    return df[(~df['queue'].str.contains("GPU|gpu", na=False)) & (df['task_attemptnr']==0)
              & (df['jobstatus']=='finished') & (df['ds_data_type']=='DAOD')
              & (df['attempt_status'].isin(['finished','done']))
              & (df['ds_scope'].str.contains("_13TeV", na=False))]
              # & (df['username'] != 'Claire Malone')]


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


# def isLocal(row):
#     return 'True' if row['site_name'] in row['ds_replicas_sites'].split(',') else 'False'
#

by = ['ds_scope']

df = pd.read_csv('FebMarch2021.csv')
filtered = filtration(df)
# filtered = filtration(df, by, most_popular(df,by))
print(filtered)

filtered['fraction_of_dataset'] = filtered['jobs_primary_input_fsize']/filtered['ds_bytes']
filtered['jobs_timetostart'] = filtered['jobs_timetostart']/60
filtered['jobs_total_duration'] = filtered['jobs_total_duration']/60
filtered['jobs_total_walltime'] = filtered['jobs_total_walltime']/60
filtered['jobs_cpuconsumptiontime'] = filtered['jobs_cpuconsumptiontime']/60
filtered['jobs_total_inputfilebytes'] = filtered['jobs_total_inputfilebytes']/1099511627776
filtered['jobs_total_outputfilebytes'] = filtered['jobs_total_outputfilebytes']/1099511627776
filtered['fraction_of_attempt_duration'] = filtered['jobs_total_duration']/(filtered['attempt_total_duration']/60)
filtered['ds_data_type_desc'] = filtered['ds_data_type_desc'].str.replace('\d+', '')


columns = list(filtered[split_by].unique())

#initialize empty dictionaries for each cloud
splitted_pivots = {}
for i in columns:
    splitted_pivots[i] = {k: v['default'] for k,v in aggregated_parameters.items()}

#initialize empty dictionary for clouds
pivot_split = {k: v['default'] for k,v in aggregated_parameters.items()}

# generating dataframes for each group
for name,meta in aggregated_parameters.items():
    queues_aggregation(filtered,
                       grouping_parameters,
                       name,
                       meta,
                       pivot_split,
                       splitted_pivots,
                       split_by,
                       level,
                       weight)

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


outputfile = f'DAOD_project_Feb-March-2021.xlsx'
writer = pd.ExcelWriter(outputfile, engine='xlsxwriter')
multiple_dict_dfs(pivot_split, split_by, 2, writer)
# multiple_dict_dfs(pivot_total, level, 2, writer)

for k,v in splitted_pivots.items():
    multiple_dict_dfs(v, k, 2, writer)

try:
    writer.save()
    print(f'Save to file {outputfile}')
except Exception as ex:
    print(ex)