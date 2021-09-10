import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from tqdm import tqdm
import math
# root = 'RawData/feb-may-2021/feb-may-2021.csv'
#
# def filtration(df):
#     return df[(df['task_attemptnr']==0)
#               & (df['jobstatus']=='finished') & (df['ds_data_type']=='DAOD')
#               & (df['attempt_status'].isin(['finished','done']))
#               & (df['ds_scope'] == 'data18_13TeV')]

df = pd.read_csv('RawData/mc16_13TeV.DAOD.TOPQ.csv')
df = df[df['ds_prod_step'] == 'deriv']


# filter single users
# unique_users = df.groupby('datasetname')['username'].nunique()
# unique_users = unique_users.reset_index()
# unique_users.sort_values(by=['username'])
# not_singles = unique_users[unique_users['username']>1]
# df = df[df['datasetname'].isin(not_singles['datasetname'].values)]


# df = df[df['ds_data_type_desc'].str.contains('TOPQ')]
# df = df[~df['datasetname'].str.contains('debug') &
#         ~df['datasetname'].str.contains('hlt') &
#         ~df['datasetname'].str.contains('scout') &
#         ~df['datasetname'].str.contains('calib')]
print(df.shape)

def date_range(start, end, intv):
    from datetime import datetime
    try:
        start = datetime.strptime(start,"%Y-%m-%d %H:%M:%S")
        end = datetime.strptime(end,"%Y-%m-%d %H:%M:%S")
        diff = (end  - start ) / intv
    except Exception as e:
        print(e)
    for i in range(intv):
        yield (start + diff * i).strftime("%Y-%m-%d %H:%M:%S")
    yield end.strftime("%Y-%m-%d %H:%M:%S")

# get min and max date
start_date = df['task_modificationtime'].min()
end_date = df['task_modificationtime'].max()
print(end_date)

n_intervals = 8

# get all dataset names
all_datasets = df['datasetname'].unique()
n_all_datasets = len(all_datasets)

all_dataset_sizes = df.groupby('datasetname')['ds_bytes'].max()/1000000000
all_dataset_sizes = all_dataset_sizes.reset_index()
all_dataset_sizes.rename(columns={'ds_bytes': 'ds_size_GB'}, inplace=True)


def get_datasets_avg_AF(df, datasets):
    min_date = df['task_modificationtime'].min()
    max_date = df['task_modificationtime'].max()
    intervals = list(date_range(min_date, max_date, n_intervals))
    # Date Intervals
    intervals_df = [df[(df['task_modificationtime'] >= intervals[i]) & (df['task_modificationtime'] < intervals[i + 1])] for i in
                    range(0, len(intervals) - 1)]
    # Aggregated Date Intervals
    agg_intervals = []
    for i, int_df in tqdm(enumerate(intervals_df)):
        # Interval weight
        weight = 2 ** (-(n_intervals - (i + 1)))
        # Search datasets within current interval
        for d in datasets:
            curr_dataset_df = int_df[int_df['datasetname'] == d]
            n_accesses = curr_dataset_df['jeditaskid'].nunique()
            n_users = curr_dataset_df['username'].nunique()
            # weighted_accesses = n_accesses * weight
            users_weight = 1/(1+math.e**(-n_users+5))
            weighted_accesses = n_accesses * weight * users_weight
            # n_replicas = curr_dataset_df['ds_replicas_number'].mean()
            # size = curr_dataset_df['ds_bytes'].mean()/1000000000
            agg_intervals.append({
                'timestamp': intervals[i+1],
                'datasetname': d,
                'n_accesses': n_accesses,
                'n_users': n_users,
                'weight': weight,
                'weighted_accesses': weighted_accesses
            })
    result = pd.DataFrame(agg_intervals)
    result[['n_accesses','n_users','weighted_accesses']].fillna(0.0, inplace=True)
    result.to_csv('ReplicationAdvisor/mc16_13TeV.DAOD.TOPQ.deriv/tasks_users/raw_AF.csv')
    return result


def get_group_avg_AF(datasets_avg_AF, n_datasets):
    # Calculate Access Frequencies for all Group of Datasets
    return datasets_avg_AF['weighted_accesses'].sum()/(n_intervals * n_datasets)


def get_actual_replicas_number(df):
    curr_replicas_info = []
    for k, v in df.groupby('datasetname')['task_modificationtime', 'ds_replicas_number']:
        curr_replicas_info.append({
            'datasetname': k,
            'curr_n_replicas': v.sort_values('task_modificationtime')['ds_replicas_number'].tail(1).values[0]
        })
    return pd.DataFrame(curr_replicas_info)


# def get_min_replicas_number(df):
#     min_replicas = df.groupby('datasetname')['ds_replicas_number'].min()
#     min_replicas = min_replicas.reset_index()
#     min_replicas.rename(columns={'ds_replicas_number': 'min_replicas_number'}, inplace=True)
#     return min_replicas
#
# min_replicas = get_min_replicas_number(df)

def get_curr_access_number(datasets_avg_AF):
    curr_accesses_info = []
    for k,v in datasets_avg_AF.groupby('datasetname'):
        curr_accesses_info.append({
            'datasetname': k,
            'curr_n_accesses': v.sort_values('timestamp')['n_accesses'].tail(1).values[0],
            'curr_n_users': v.sort_values('timestamp')['n_users'].tail(1).values[0]
        })
    return pd.DataFrame(curr_accesses_info)


def optimal_n_replicas(datasets_avg_AF, groupAF, actual_n_replicas, dataset_sizes, actual_n_accesses):
    # Create Dataframe with Average Datasets Access Frequencies
    datasetsAF = datasets_avg_AF.groupby('datasetname')['weighted_accesses'].sum()/n_intervals
    datasetsAF = datasetsAF.reset_index()
    datasetsAF.rename(columns={'weighted_accesses': 'datasetAF'}, inplace=True)
    datasetsAF['groupAF'] = groupAF
    datasetsAF = pd.merge(datasetsAF, actual_n_replicas, how='left', left_on=['datasetname'],
                          right_on=['datasetname'])
    datasetsAF = pd.merge(datasetsAF, actual_n_accesses, how='left', left_on=['datasetname'],
                          right_on=['datasetname'])
    # datasetsAF = pd.merge(datasetsAF, min_replicas, how='left', left_on=['datasetname'],
    #                       right_on=['datasetname'])
    decision = round(datasetsAF['datasetAF'] / groupAF)
    # Note:
    # if the optimal number of replicas is 0
    # it is set to the 1
    datasetsAF['optimal_n_replicas'] = decision
    datasetsAF['optimal_n_replicas'][datasetsAF['optimal_n_replicas'] > datasetsAF['curr_n_accesses']] = datasetsAF[
        'curr_n_accesses']
    datasetsAF['optimal_n_replicas'][datasetsAF['optimal_n_replicas'] == 0.0] = 1

    datasetsAF = pd.merge(datasetsAF, dataset_sizes, how='left', left_on=['datasetname'],
                          right_on=['datasetname'])
    datasetsAF['timestamp'] = datasets_avg_AF['timestamp'].max()

    return datasetsAF


def simulation_step(df, datasets, dataset_sizes):
    datasets_avg_AF = get_datasets_avg_AF(df,datasets)
    groupAF = get_group_avg_AF(datasets_avg_AF,len(datasets))
    return optimal_n_replicas(datasets_avg_AF, groupAF,
                                    get_actual_replicas_number(df),
                                    dataset_sizes,
                                    get_curr_access_number(datasets_avg_AF))


def add_date(date, n_of_days = 7):
    return (datetime.strptime(date, '%Y-%m-%d %H:%M:%S') + timedelta(n_of_days)).strftime('%Y-%m-%d %H:%M:%S')


# simulation (run by week)
curr_date = start_date
while curr_date < end_date:
    interval_start = curr_date
    interval_end = add_date(interval_start, 60)
    print(f'Interval start: {interval_start}')
    print(f'Interval end: {interval_end}')
    tmp_df = df[(df['task_modificationtime']>=interval_start)
                           & (df['task_modificationtime'] < interval_end)]
    # # get dataset names
    # datasets = tmp_df['datasetname'].unique()
    # n_datasets = len(datasets)
    # dataset_sizes = tmp_df.groupby('datasetname')['ds_bytes'].max() / 1000000000
    # dataset_sizes = dataset_sizes.reset_index()
    # dataset_sizes.rename(columns={'ds_bytes': 'ds_size_GB'}, inplace=True)

    try:
        simulation_step(tmp_df,all_datasets,all_dataset_sizes).to_csv(f'ReplicationAdvisor/mc16_13TeV.DAOD.TOPQ.deriv/tasks_users/{interval_end}.csv')
        print(f'ReplicationAdvisor/mc16_13TeV.DAOD.TOPQ.deriv/tasks_users/{interval_end}.csv has been saved')
    except Exception as e:
        print(e)
    curr_date = add_date(curr_date, 7)
