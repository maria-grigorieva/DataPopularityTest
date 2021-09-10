import pandas as pd
import numpy as np
import glob
import plotly.express as px
import ast
import xlsxwriter


def most_popular(df, by):
    """
    Get the most popular datasets for the analysis
    :param df:
    :param by:
    :return:
    """
    popular = df.groupby(by[0])['jeditaskid'].nunique()
    popular = popular.sort_values(ascending=False)
    return popular.index[0:500]


def merge(path):
    all_files = glob.glob(path)
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    return pd.concat(li, axis=0, ignore_index=True)

def filtration(df, by=None, popular=None):
    if popular is not None:
        df = df[df[by[0]].isin(popular)]
    return df[(~df['queue'].str.contains("GPU|gpu", na=False)) & (df['task_attemptnr']==0)
              & (df['jobstatus']=='finished') & (df['ds_data_type']=='DAOD')
              & (df['attempt_status'].isin(['finished','done']))
              & (df['ds_scope'].str.contains("_13TeV", na=False))]
              # & (df['username'] != 'Claire Malone')]


def to_pivot(df, by, level, target, targets, agg=False):
    aggfunc = {'wavg':np.mean,
               'mean':np.mean,
               'sum':np.sum,
               'nunique':np.unique}
    return df.pivot_table(index=by, columns=level, values=target, aggfunc=aggfunc[targets[target]]) \
        if agg else df.pivot(index=by[0], columns=level, values=target)


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


def custom_round(x, base=5):
    return int(base * round(float(x)/base))

def queues_aggregation(df, by, target, targets,
                       pivot_split,
                       # pivot_total,
                       splitted_pivots,
                       split_by=None,
                       level='site_name',
                       aggfunc='mean',
                       weights='number_of_jobs'):
    print(f'First regroup by {by} and {level} for {target}')
    group = by + [split_by, level] if split_by is not None else by + [level]
    if aggfunc == 'mean':
        res = df.groupby(group)[target].mean().reset_index()
    elif aggfunc == 'sum':
        res = df.groupby(group)[target].sum().reset_index()
    elif aggfunc == 'median':
        res = df.groupby(group)[target].median().reset_index()
    elif aggfunc == 'wavg':
        res = df.groupby(group + [target])[weights].sum().reset_index()
        res = res.groupby(group)[target, weights].apply(lambda x: wavg(x, target, weights)).reset_index(name=target)
    elif aggfunc == ' q90':
        res = df.groupby(group)[target].quantile(0.9).reset_index()
    elif aggfunc == 'nunique':
        res = df.groupby(group)[target].nunique().reset_index()

    print(f'Number of groups for {target}:')
    print(res.shape[0])
    print(f'Second regroup by {by} for {level} and {target}')
    if split_by is not None:
        split_df = res.groupby(split_by)
        for name, gr in split_df:
            # tmp = to_pivot(gr, by, level, target).round(decimals=3)
            # tmp = tmp.sort_values(by=list(tmp.columns)[0], ascending=False)
            splitted_pivots[name][target] = to_pivot(gr, by, level, target, targets).round(decimals=1)

        # pivot_total[target] = to_pivot(res, by, level, target).round(decimals=3)
        # pivot_total[target] = pivot_total[target].sort_values(by=list(pivot_total[target].columns)[0], ascending=False)

        pivot_split[target] = to_pivot(res, by, split_by, target, targets, agg=True).round(decimals=1)
        # pivot_split[target] = pivot_split[target].sort_values(by=list(pivot_split[target].columns)[0], ascending=False)

# df = merge('/Users/maria/PycharmProjects/data-popularity/test-data/from_aiatlas007/*.csv')
# df.to_csv('February2021.csv')


by = ['ds_scope']
#by = 'ds_scope'
# target = 'number_of_jobs'
# aggfunc='sum'
level='site_name'
split_by='site_cloud'
weight = 'number_of_jobs'

df = pd.read_csv('FebMarch2021.csv')
filtered = filtration(df)
# filtered = filtration(df, by, most_popular(df,by))
print(filtered)

filtered['fraction_of_dataset'] = filtered['jobs_primary_input_fsize']/filtered['ds_bytes']
filtered['jobs_timetostart'] = filtered['jobs_timetostart']/60
filtered['jobs_total_duration'] = filtered['jobs_total_duration']/60
filtered['jobs_total_walltime'] = filtered['jobs_total_walltime']/60
filtered['jobs_cpuconsumptiontime'] = filtered['jobs_cpuconsumptiontime']/60
filtered['jobs_total_inputfilebytes'] = filtered['jobs_total_inputfilebytes']/1000000000000
filtered['jobs_total_outputfilebytes'] = filtered['jobs_total_outputfilebytes']/1000000000000
filtered['fraction_of_attempt_duration'] = filtered['jobs_total_duration']/(filtered['attempt_total_duration']/60)
filtered['ds_data_type_desc'] = filtered['ds_data_type_desc'].str.replace('\d+', '')

columns = list(filtered[split_by].unique())

targets = {
    'jobs_total_duration': 'wavg',
    'jobs_timetostart': 'wavg',
    'number_of_jobs': 'sum',
    'fraction_of_dataset': 'wavg',
    'jobs_total_walltime': 'wavg',
    'jobs_cpuconsumptiontime': 'wavg',
    'jobs_total_inputfilebytes': 'sum',
    'jobs_total_outputfilebytes': 'sum',
    'fraction_of_attempt_duration': 'mean',
    # 'username': 'nunique',
    # 'institute_name': 'nunique',
    # 'institute_country': 'nunique'
}

empty_dict = {}
for i in targets.keys():
    empty_dict[i] = None

splitted_pivots = {}
for i in columns:
    splitted_pivots[i] = {
    'jobs_total_duration': None,
    'jobs_timetostart': None,
    'number_of_jobs': None,
    'fraction_of_dataset': None,
    'jobs_total_walltime': None,
    'jobs_cpuconsumptiontime': None,
    'jobs_total_inputfilebytes': None,
    'jobs_total_outputfilebytes': None,
    'fraction_of_attempt_duration': None,
    # 'username': None,
    # 'institute_name': None,
    # 'institute_country': None
}
pivot_split = {
    'jobs_total_duration': None,
    'jobs_timetostart': None,
    'number_of_jobs': None,
    'fraction_of_dataset': None,
    'jobs_total_walltime': None,
    'jobs_cpuconsumptiontime': None,
    'jobs_total_inputfilebytes': None,
    'jobs_total_outputfilebytes': None,
    'fraction_of_attempt_duration': None,
    # 'username': None,
    # 'institute_name': None,
    # 'institute_country': None
}
# pivot_total = {
#     'jobs_total_duration': None,
#     'jobs_timetostart': None,
#     'number_of_jobs': None,
#     'fraction_of_dataset': None,
#     'jobs_total_walltime': None,
#     'jobs_cpuconsumptiontime': None,
#     'jobs_total_inputfilebytes': None,
#     'jobs_total_outputfilebytes': None,
#     'fraction_of_attempt_duration': None,
#     'username': None,
#     'institute_name': None,
#     'instititu_country': None
# }

for k,v in targets.items():
    queues_aggregation(filtered, by, k, targets, pivot_split, splitted_pivots, split_by, level, v, weight)

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


outputfile = f'DAOD_{by[0]}_Feb-March-2021.xlsx'
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


