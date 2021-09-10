import pandas as pd
import numpy as np
import glob
import plotly.express as px
import ast
import xlsxwriter

def merge(path):
    all_files = glob.glob(path)
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    return pd.concat(li, axis=0, ignore_index=True)

def filtration(df):
    return df[(~df['queue'].str.contains("GPU|gpu", na=False)) & (df['task_attemptnr']==0)
              & (df['jobstatus']=='finished') & (df['ds_data_type']=='DAOD')
              & (df['attempt_status'].isin(['finished','done']))]


def to_pivot(df, by, level, target, agg=False):
    return df.pivot_table(index=by, columns=level, values=target, aggfunc=np.mean) \
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

def queues_aggregation(df, by, target, outputfile,
                       split_by=None,
                       level='site_name',
                       aggfunc='mean',
                       weights='number_of_jobs',
                       limit=500):
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
    print(f'Number of groups: {res.shape[0]}')
    print(f'Second regroup by {by} for {level} and {target}')
    if split_by is not None:
        writer = pd.ExcelWriter(outputfile, engine='xlsxwriter')
        split_df = res.groupby(split_by)
        for name, gr in split_df:
            pivot = to_pivot(gr, by, level, target)
            pivot.head(limit).style.background_gradient(cmap='Blues').to_excel(writer, sheet_name=name)
        pivot_total = to_pivot(res, by, level, target)
        pivot_total.head(limit).style.background_gradient(cmap='Blues').to_excel(writer, sheet_name=level)
        pivot_split = to_pivot(res, by, split_by, target, agg=True)
        pivot_split.head(limit).style.background_gradient(cmap='Blues').to_excel(writer, sheet_name=split_by)
        writer.save()
    else:
        pivot = to_pivot(res, by, level, target)
        pivot.head(limit).style.background_gradient(cmap='viridis').to_excel(outputfile)

# df = merge('/Users/maria/PycharmProjects/data-popularity/test-data/from_aiatlas007/*.csv')
# df.to_csv('February2021.csv')
df = pd.read_csv('February2021.csv')
filtered = filtration(df)
print(filtered)

filtered['fraction_of_dataset'] = filtered['jobs_primary_input_fsize']/filtered['ds_bytes']
filtered['jobs_timetostart'] = filtered['jobs_timetostart']/60
# filtered['jobs_timetostart'] = filtered['jobs_timetostart'].apply(lambda x: custom_round(x, base=10))

by = ['ds_scope']
#by = 'ds_scope'
target = 'number_of_jobs'
aggfunc='sum'
level='site_name'
split_by='site_cloud'
weight = 'number_of_jobs'
outputfile = f'DAOD_{by[0]}_{target}_{aggfunc}.xlsx'

queues_aggregation(filtered, by, target, outputfile, split_by, level, aggfunc, weight)