import pandas as pd
import math
# ds_project : "mc16_13TeV" and
# ds_data_type : "DAOD" and
# ds_data_type_desc : SUSY* and
# task_attemptnr : "0" and
# jobstatus : "finished" and
# ds_prod_step : "deriv"

def filtration(df):
    return df[(df['task_attemptnr']==0)
              & (df['jobstatus']=='finished') & (df['ds_data_type']=='DAOD')
              & (df['ds_data_type_desc'] == 'SUSY5') & (df['ds_prod_step'] == 'deriv')
              & (df['attempt_status'].isin(['finished','done']))
              & (df['ds_scope'] == 'mc16_13TeV')]

def date_range(start, end, intv):
    from datetime import datetime
    start = datetime.strptime(start,"%Y-%m-%d %H:%M:%S")
    end = datetime.strptime(end,"%Y-%m-%d %H:%M:%S")
    diff = (end  - start ) / intv
    for i in range(intv):
        yield (start + diff * i).strftime("%Y-%m-%d %H:%M:%S")
    yield end.strftime("%Y-%m-%d %H:%M:%S")

#
# df = pd.read_csv('FebMarch2021.csv')
# filtered = filtration(df)
# filtered[['datasetname','attempt_start',
#           'number_of_jobs','ds_replicas_number',
#           'site_name','site_cloud','jeditaskid',
#           'ds_replicas_clouds','ds_replicas_sites']].to_csv('mc16_13TeV.DAOD.deriv.SUSY5_prepared.csv')
# print('filtered file saved')
df = pd.read_csv('mc16_13TeV.DAOD.deriv.SUSY5_prepared.csv')

grouped = df.groupby('datasetname')

datasets_af_intervals = []
datasets_clouds_avg_af = []
datasets_sites_avg_af = []
clouds = df['site_cloud'].unique()
sites = df['site_name'].unique()
datasets_avg_af = []
n_datasets = len(grouped)
print(n_datasets)
n_intervals = 10
current_n_accesses = 0

min_date = df['attempt_start'].min()
max_date = df['attempt_start'].max()
intervals = list(date_range(min_date, max_date, n_intervals))

# calculations for each datasetname
for k,v in grouped:
    # clouds = []
    dataset = k
    interval_agg = []
    clouds_agg = []
    sites_agg = []
    current_n_accesses = v[(v['attempt_start'] >= intervals[len(intervals)-2]) &
                        (v['attempt_start'] < intervals[len(intervals)-1])]['jeditaskid'].nunique()
    current_n_replicas = v[v['attempt_start'] == v['attempt_start'].max()]['ds_replicas_number'].mean()

    # aggregation by time intervals for the dataset
    for i in range(0,len(intervals)-1):
        curr_interval = v[(v['attempt_start'] >= intervals[i]) & (v['attempt_start'] < intervals[i+1])]
        n_accesses_per_interval = curr_interval['jeditaskid'].nunique()
        # weight_linear = (i+1)/len(intervals)
        # weight_exp = math.exp(-(i+1)/len(intervals))
        weight_LALV = 2**(-(len(intervals)-(i+1)))
        interval_agg.append({
            'datasetname': k,
            'timestamp': intervals[i],
            'weight': weight_LALV,
            'n_accesses': n_accesses_per_interval,
            'weighted_accesses': n_accesses_per_interval*weight_LALV
        })

        # dataset - clouds - time interval
        for c in clouds:
            v_cloud = curr_interval[curr_interval['site_cloud'] == c]
            clouds_agg.append({
                'datasetname': k,
                'timestamp': intervals[i],
                'cloud': c,
                'weighted_accesses': v_cloud['jeditaskid'].nunique() * weight_LALV
            })
        # dataset - sites - time interval
        for s in sites:
            v_site = curr_interval[curr_interval['site_name'] == s]
            sites_agg.append({
                'datasetname': k,
                'timestamp': intervals[i],
                'site': s,
                'weighted_accesses': v_site['jeditaskid'].nunique() * weight_LALV
            })

    # AVG Access Frequency for Dataset
    dataset_avg_AF_LALV = pd.DataFrame(interval_agg)['weighted_accesses'].sum()/n_intervals

    datasets_avg_af.append({'datasetname': k,
                           'current_n_replicas': current_n_replicas,
                           'current_accesses': current_n_accesses,
                           'ds_avg_access_frequency': dataset_avg_AF_LALV
                           })
    # AVG Access Frequency for Dataset at Cloud
    clouds_agg_df = pd.DataFrame(clouds_agg)
    sites_agg_df = pd.DataFrame(sites_agg)
    clouds_agg_df = clouds_agg_df.groupby('cloud').sum()/n_intervals
    clouds_agg_df = clouds_agg_df.reset_index()
    clouds_agg_df['datasetname'] = k
    overall_accesses_clouds = clouds_agg_df['weighted_accesses'].sum()
    clouds_agg_df['x_clouds'] = clouds_agg_df['weighted_accesses']/overall_accesses_clouds

    sites_agg_df = sites_agg_df.groupby('site').sum()/n_intervals
    sites_agg_df = sites_agg_df.reset_index()
    sites_agg_df['datasetname'] = k
    overall_accesses_sites = sites_agg_df['weighted_accesses'].sum()
    sites_agg_df['x_sites'] = sites_agg_df['weighted_accesses']/overall_accesses_sites

    datasets_clouds_avg_af.append(clouds_agg_df)
    datasets_sites_avg_af.append(sites_agg_df)

    datasets_af_intervals.extend(interval_agg)

# calculate avg access frequency for group of datasets
datasets_af_intervals_df = pd.DataFrame(datasets_af_intervals)
datasets_af_intervals_df.to_csv('datasets_raw.csv')
group_avg_AF_LALV = datasets_af_intervals_df['weighted_accesses'].sum()/(n_intervals*n_datasets)

avg_af_df = pd.DataFrame(datasets_avg_af)
avg_af_df['group_avg_access_frequency'] = group_avg_AF_LALV
avg_af_df['optimal_replicas_number'] = round(avg_af_df['ds_avg_access_frequency']/group_avg_AF_LALV)

avg_af_df.to_csv('result_n_replicas.csv')

dataset_avg_AF_LALV_cloud_df = pd.concat(datasets_clouds_avg_af)
result_clouds = pd.merge(dataset_avg_AF_LALV_cloud_df, avg_af_df, how='left', left_on=['datasetname'], right_on=['datasetname'])
result_clouds['n_replicas_at_cloud'] = round(result_clouds['optimal_replicas_number']*(result_clouds['x_clouds']))

dataset_avg_AF_LALV_site_df = pd.concat(datasets_sites_avg_af)
result_sites = pd.merge(dataset_avg_AF_LALV_site_df, avg_af_df,how='left', left_on=['datasetname'], right_on=['datasetname'])
result_sites['n_replicas_at_site'] = round(result_sites['optimal_replicas_number']*(result_sites['x_sites']))

result_sites.to_csv('result_sites.csv')
result_clouds.to_csv('result_clouds.csv')
print('saved')

