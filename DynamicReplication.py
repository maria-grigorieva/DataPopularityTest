import pandas as pd

def date_range(start, end, intv):
    from datetime import datetime
    start = datetime.strptime(start,"%Y-%m-%d %H:%M:%S")
    end = datetime.strptime(end,"%Y-%m-%d %H:%M:%S")
    diff = (end  - start ) / intv
    for i in range(intv):
        yield (start + diff * i).strftime("%Y-%m-%d %H:%M:%S")
    yield end.strftime("%Y-%m-%d %H:%M:%S")

df = pd.read_csv('RawData/mc16_13TeV.DAOD.deriv.SUSY5_prepared.csv')

min_date = df['attempt_start'].min()
max_date = df['attempt_start'].max()

n_intervals = 8
intervals = list(date_range(min_date, max_date, n_intervals))

# Date Intervals
intervals_df = [df[(df['attempt_start'] >= intervals[i]) & (df['attempt_start'] < intervals[i + 1])] for i in range(0, len(intervals) - 1)]

# Aggregated Date Intervals
agg_intervals = []

# Dataset Names
datasetnames = df['datasetname'].unique()
n_datasets = df['datasetname'].nunique()

# Get current number of dataset replicas
curr_replicas_info = []
for k,v in df.groupby('datasetname')['attempt_start','ds_replicas_number']:
    curr_replicas_info.append({
        'datasetname': k,
        'curr_n_replicas': v.sort_values('attempt_start')['ds_replicas_number'].tail(1).values[0]
    })
curr_replicas_info_df = pd.DataFrame(curr_replicas_info)


# Loop All Date Intervals
for i,int_df in enumerate(intervals_df):
    # Interval weight
    weight = 2**(-(n_intervals-(i+1)))
    # Search datasets within current interval
    for d in datasetnames:
        curr_dataset_df = int_df[int_df['datasetname'] == d]
        n_accesses = curr_dataset_df['jeditaskid'].nunique()
        weighted_accesses = n_accesses * weight
        agg_intervals.append({
            'timestamp': intervals[i],
            'datasetname': d,
            'n_accesses': n_accesses,
            'weight': weight,
            'weighted_accesses': n_accesses * weight
        })

agg_intervals_df = pd.DataFrame(agg_intervals)

# Calculate Access Frequencies for all Group of Datasets
groupAF = agg_intervals_df['weighted_accesses'].sum()/(n_intervals*n_datasets)

# Create Dataframe with Average Datasets Access Frequencies
datasetsAF = agg_intervals_df.groupby('datasetname')['weighted_accesses'].sum()/n_intervals
datasetsAF = datasetsAF.reset_index()
datasetsAF.rename(columns={'weighted_accesses':'datasetAF'}, inplace=True)
datasetsAF['groupAF'] = groupAF
datasetsAF = pd.merge(datasetsAF, curr_replicas_info_df, how='left', left_on=['datasetname'], right_on=['datasetname'])
datasetsAF['optimal_n_replicas'] = round(datasetsAF['datasetAF']/groupAF)
datasetsAF['decision'] = datasetsAF['optimal_n_replicas']-datasetsAF['curr_n_replicas']

# get current number of accesses
curr_accesses_info = []
for k,v in agg_intervals_df.groupby('datasetname'):
    curr_accesses_info.append({
        'datasetname': k,
        'curr_n_accesses': v.sort_values('timestamp')['n_accesses'].tail(1).values[0]
    })
curr_accesses_info_df = pd.DataFrame(curr_accesses_info)

datasetsAF = pd.merge(datasetsAF, curr_accesses_info_df, how='left', left_on=['datasetname'], right_on=['datasetname'])

print(datasetsAF)
datasetsAF.to_csv('datasetsAF.csv')
print('saved!')

# agg_intervals_df.to_csv('agg_intervals_df.csv')
# print('saved!')





