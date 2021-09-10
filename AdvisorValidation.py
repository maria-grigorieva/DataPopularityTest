import pandas as pd
import os

rawData = 'RawData/Apr-2021/FirstWeek/first_week.csv'
root_dir = "ReplicationAdvisor/mc16_13TeV.DAOD.deriv.SUSY5"  # Path relative to current dir (os.getcwd())
#files = [item for item in os.listdir(root_dir) if os.path.isfile(os.path.join(root_dir, item))]  # Filter items and only keep files (strip out directories)
#n_intervals = 4

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

def filtration(df, project, data_type):
    return df[(df['task_attemptnr']==0)
              & (df['jobstatus']=='finished') & (df['ds_data_type']==data_type)
              & (df['attempt_status'].isin(['finished','done']))
              & (df['ds_scope'] == project)]

def validator(df, n_intervals, datasets):
    min_date = df['attempt_start'].min()
    max_date = df['attempt_start'].max()

    intervals = list(date_range(min_date, max_date, n_intervals))
    # Date Intervals
    intervals_df = [df[(df['attempt_start'] >= intervals[i]) & (df['attempt_start'] < intervals[i + 1])] for i in
                    range(0, len(intervals) - 1)]
    # Aggregated Date Intervals
    agg_intervals = []
    # Dataset Names
    n_datasets = len(datasets)

    # Loop All Date Intervals
    for i, int_df in enumerate(intervals_df):
        # Interval weight
        weight = 2 ** (-(n_intervals - (i + 1)))
        # Search datasets within current interval
        for d in datasets:
            curr_dataset_df = int_df[int_df['datasetname'] == d]
            n_accesses = curr_dataset_df['jeditaskid'].nunique()
            # n_users = curr_dataset_df['username'].nunique()
            agg_intervals.append({
                'timestamp': intervals[i],
                'datasetname': d,
                'n_accesses': n_accesses,
                'weight': weight,
                'weighted_accesses': n_accesses * weight
            })

    agg_intervals_df = pd.DataFrame(agg_intervals)
    # Calculate Access Frequencies for all Group of Datasets
    groupAF = agg_intervals_df['weighted_accesses'].sum() / (n_intervals * n_datasets)
    # Create Dataframe with Average Datasets Access Frequencies
    datasetsAF = agg_intervals_df.groupby('datasetname')['weighted_accesses'].sum() / n_intervals
    datasetsAF = datasetsAF.reset_index()
    datasetsAF.rename(columns={'weighted_accesses': 'new_datasetAF'}, inplace=True)
    datasetsAF['new_groupAF'] = groupAF
    return datasetsAF


# raw_files = [item for item in os.listdir(rawData) if os.path.isfile(os.path.join(rawData, item))]
# new_df = [r for r in raw_files]
# new_df = pd.concat(new_df)
#
# for f in files:
#     old_df = pd.read_csv(f'{root_dir}/{f}')
#     project = f.split('-')[0]
#     data_type = f.split('-')[1][:-4]
#     datasets = old_df['datasetname'].unique()
#
#     validation_df = validator(filtration(new_df, project, data_type), n_intervals, datasets)
#
#     result = pd.merge(old_df, validation_df, how='left', left_on=['datasetname'],
#                           right_on=['datasetname'])
#     result.to_csv(f'ReplicationAdvisor/Tasks_Feb-March-2021/Validated/{f}')

df = pd.read_csv(rawData)
df = df[(df['task_attemptnr']==0)
      & (df['jobstatus']=='finished') & (df['ds_data_type']=='DAOD')
      & (df['attempt_status'].isin(['finished','done']))
      & (df['ds_scope'] == 'mc16_13TeV')
      & (df['ds_data_type_desc']=='SUSY5')]

old_df = pd.read_csv(f'datasetsAF.csv')

datasets = df.groupby('datasetname')['jeditaskid'].nunique()
datasets = datasets.reset_index()
datasets.rename(columns={'jeditaskid':'next_week_accesses'}, inplace=True)

result = pd.merge(old_df, datasets, how='left', left_on=['datasetname'], right_on=['datasetname'])
result['validator'] = result.apply(lambda row: (row['next_week_accesses']-row['curr_n_accesses'])>0 and row['decision']>0)
result.to_csv('validated_datasets.csv')
