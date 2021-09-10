import pandas as pd
import numpy as np
from datetime import datetime, timedelta
# root = 'RawData/feb-may-2021/feb-may-2021.csv'
#
def filtration(df):
    return df[(df['task_attemptnr']==0)
              & (df['jobstatus']=='finished') & (df['ds_data_type']=='DAOD')
              & (df['attempt_status'].isin(['finished','done']))
              & (df['ds_scope'] == 'mc16_13TeV')
              & (df['ds_data_type_desc'].str.contains('TOPQ'))
              & (~df['datasetname'].str.contains('debug')) &
              (~df['datasetname'].str.contains('hlt')) &
              (~df['datasetname'].str.contains('scout')) &
              (~df['datasetname'].str.contains('calib'))]

df = pd.read_csv('RawData/feb-may-2021/feb-may-2021.csv')
filtered = filtration(df)
filtered.to_csv('mc16_13TeV.DAOD.TOPQ.csv')
print('saved!')


# df = pd.read_csv('RawData/feb-may-2021/data18_13TeV-DAOD.csv')
# df['attempt_start'].dropna(inplace=True)
#
# def date_range(start, end, intv):
#     from datetime import datetime
#     try:
#         start = datetime.strptime(start,"%Y-%m-%d %H:%M:%S")
#         end = datetime.strptime(end,"%Y-%m-%d %H:%M:%S")
#         diff = (end  - start ) / intv
#     except Exception as e:
#         print(e)
#     for i in range(intv):
#         yield (start + diff * i).strftime("%Y-%m-%d %H:%M:%S")
#     yield end.strftime("%Y-%m-%d %H:%M:%S")
#
# # get min and max date
# start_date = df['task_modificationtime'].min()
# end_date = df['task_modificationtime'].max()
# # get the first statistics on 2 months
# two_months_finished = (datetime.strptime(start_date, '%Y-%m-%d %H:%M:%S') + timedelta(60)).strftime('%Y-%m-%d %H:%M:%S')
# print(two_months_finished)
#
# n_intervals = 8
#
# # get all dataset names
# all_datasets = df['datasetname'].unique()
# n_all_datasets = len(all_datasets)
#
# # test 2 months (Feb + March)
# test_df = df[(df['task_modificationtime']>=start_date) & (df['task_modificationtime'] < two_months_finished)]
# # validation 2 months (Apr + May)
# validation_df = df[(df['task_modificationtime']>=two_months_finished) & (df['task_modificationtime'] <= end_date)]
#
#
# all_dataset_sizes = df.groupby('datasetname')['ds_bytes'].max()/1000000000
#
# def get_datasets_avg_AF(df):
#     min_date = df['task_modificationtime'].min()
#     max_date = df['task_modificationtime'].max()
#     intervals = list(date_range(min_date, max_date, n_intervals))
#     # Date Intervals
#     intervals_df = [df[(df['task_modificationtime'] >= intervals[i]) & (df['task_modificationtime'] < intervals[i + 1])] for i in
#                     range(0, len(intervals) - 1)]
#     # Aggregated Date Intervals
#     agg_intervals = []
#     for i, int_df in enumerate(intervals_df):
#         # Interval weight
#         weight = 2 ** (-(n_intervals - (i + 1)))
#         # Search datasets within current interval
#         for d in all_datasets:
#             curr_dataset_df = int_df[int_df['datasetname'] == d]
#             n_accesses = curr_dataset_df['jeditaskid'].nunique()
#             weighted_accesses = n_accesses * weight
#             # n_replicas = curr_dataset_df['ds_replicas_number'].mean()
#             # size = curr_dataset_df['ds_bytes'].mean()/1000000000
#             agg_intervals.append({
#                 'timestamp': intervals[i],
#                 'datasetname': d,
#                 'n_accesses': n_accesses,
#                 'weight': weight,
#                 'weighted_accesses': weighted_accesses
#             })
#     return pd.DataFrame(agg_intervals)
#
#
# def get_group_avg_AF(datasets_avg_AF):
#     # Calculate Access Frequencies for all Group of Datasets
#     return datasets_avg_AF['weighted_accesses'].sum()/(n_intervals * n_all_datasets)
#
#
# def get_actual_replicas_number(df):
#     curr_replicas_info = []
#     for k, v in df.groupby('datasetname')['task_modificationtime', 'ds_replicas_number']:
#         curr_replicas_info.append({
#             'datasetname': k,
#             'curr_n_replicas': v.sort_values('task_modificationtime')['ds_replicas_number'].tail(1).values[0]
#         })
#     return pd.DataFrame(curr_replicas_info)
#
#
# def get_curr_access_number(datasets_avg_AF):
#     curr_accesses_info = []
#     for k,v in datasets_avg_AF.groupby('datasetname'):
#         curr_accesses_info.append({
#             'datasetname': k,
#             'curr_n_accesses': v.sort_values('timestamp')['n_accesses'].tail(1).values[0]
#         })
#     return pd.DataFrame(curr_accesses_info)
#
#
# def optimal_n_replicas(datasets_avg_AF, groupAF, actual_n_replicas, dataset_sizes, actual_n_accesses):
#     # Create Dataframe with Average Datasets Access Frequencies
#     datasetsAF = datasets_avg_AF.groupby('datasetname')['weighted_accesses'].sum()/n_intervals
#     datasetsAF = datasetsAF.reset_index()
#     datasetsAF.rename(columns={'weighted_accesses': 'datasetAF'}, inplace=True)
#     datasetsAF['groupAF'] = groupAF
#     datasetsAF = pd.merge(datasetsAF, actual_n_replicas, how='left', left_on=['datasetname'],
#                           right_on=['datasetname'])
#     datasetsAF['optimal_n_replicas'] = round(datasetsAF['datasetAF'] / groupAF)
#     datasetsAF['decision'] = datasetsAF['optimal_n_replicas'] - datasetsAF['curr_n_replicas']
#     datasetsAF = pd.merge(datasetsAF, dataset_sizes, how='left', left_on=['datasetname'],
#                           right_on=['datasetname'])
#     datasetsAF['timestamp'] = datasets_avg_AF['timestamp'].max()
#     datasetsAF = pd.merge(datasetsAF, actual_n_accesses, how='left', left_on=['datasetname'],
#                           right_on=['datasetname'])
#     return datasetsAF
#
#
# # Run frist stage: training
# datasets_avg_AF = get_datasets_avg_AF(test_df)
# groupAF = get_group_avg_AF(datasets_avg_AF)
# datasetsAF = optimal_n_replicas(datasets_avg_AF, groupAF,
#                                 get_actual_replicas_number(test_df),
#                                 all_dataset_sizes,
#                                 get_curr_access_number(datasets_avg_AF))
# datasetsAF.to_csv('ReplicationAdvisor/data18_13TeV.DAOD/trained_df.csv')



