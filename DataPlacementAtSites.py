import pandas as pd

def date_range(start, end, intv):
    from datetime import datetime
    start = datetime.strptime(start,"%Y-%m-%d %H:%M:%S")
    end = datetime.strptime(end,"%Y-%m-%d %H:%M:%S")
    diff = (end  - start ) / intv
    for i in range(intv):
        yield (start + diff * i).strftime("%Y-%m-%d %H:%M:%S")
    yield end.strftime("%Y-%m-%d %H:%M:%S")

df = pd.read_csv('mc16_13TeV.DAOD.deriv.SUSY5_prepared.csv')

min_date = df['attempt_start'].min()
max_date = df['attempt_start'].max()

n_intervals = 10
intervals = list(date_range(min_date, max_date, n_intervals))

# Date Intervals
intervals_df = [df[(df['attempt_start'] >= intervals[i]) & (df['attempt_start'] < intervals[i + 1])] for i in range(0, len(intervals) - 1)]

# Aggregated Date Intervals
agg_intervals = []

# Dataset Names
datasetnames = df['datasetname'].unique()
n_datasets = df['datasetname'].nunique()

# clouds
clouds = df['site_cloud'].unique()

# sites
sites = df['site_name'].unique()

# Loop All Date Intervals
for i,int_df in enumerate(intervals_df):
    # Interval weight
    weight = 2**(-(n_intervals-(i+1)))
    # Search datasets within current interval
    for d in datasetnames:
        curr_dataset_df = int_df[int_df['datasetname'] == d]
        if curr_dataset_df.empty != True:
            # N of Accesses per Clouds
            for s in sites:
                curr_cloud_df = curr_dataset_df[curr_dataset_df['site_name']==s]
                agg_intervals.append(
                    {
                        'timestamp': intervals[i],
                        'datasetname': d,
                        'site': s,
                        'weighted_accesses': curr_cloud_df['jeditaskid'].nunique()*weight
                    }
                )

agg_intervals_df = pd.DataFrame(agg_intervals)

# Calculate Avg Access Frequency of Each Dataset in Cloud
datasets_sitesAF = agg_intervals_df.groupby(['datasetname','site'])['weighted_accesses'].sum()/n_intervals
datasets_sitesAF = datasets_sitesAF.reset_index()
datasets_sitesAF.rename(columns={'weighted_accesses':'datasetAF_site'}, inplace=True)

datasets_n_replicas_df = pd.read_csv('datasetsAF.csv')

# Calculate Avg Access Frequency of Each Dataset in All Clouds
datasetsAF = datasets_sitesAF.groupby('datasetname')['datasetAF_site'].sum()
datasetsAF = datasetsAF.reset_index()
datasetsAF.rename(columns={'datasetAF_site':'datasetAF_all_sites'}, inplace=True)
datasetsAF = pd.merge(datasetsAF, datasets_sitesAF, how='left', left_on=['datasetname'], right_on=['datasetname'])
datasetsAF = pd.merge(datasetsAF, datasets_n_replicas_df, how='left', left_on=['datasetname'], right_on=['datasetname'])
datasetsAF['replicas_at_site'] = round(datasetsAF['optimal_n_replicas']*(datasetsAF['datasetAF_site']/datasetsAF['datasetAF_all_sites']))
#datasetsAF['decision'] = datasetsAF['optimal_n_replicas']-datasetsAF['curr_n_replicas']

print(datasetsAF)
datasetsAF.to_csv('datasetsAF_sites.csv')
print('saved!')