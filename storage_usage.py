import pandas as pd

def filtration(df):
    return df[(~df['queue'].str.contains("GPU|gpu", na=False)) & (df['task_attemptnr']==0)
              & (df['jobstatus']=='finished') & (df['ds_data_type']=='DAOD')
              & (df['attempt_status'].isin(['finished','done']))
              & (df['ds_scope'].str.contains("_13TeV", na=False))]


df = pd.read_csv('February2021_updated.csv')
filtered = filtration(df)

filtered['ds_data_type_desc'] = filtered['ds_data_type_desc'].str.replace('\d+', '')
filtered['ds_bytes'] = filtered['ds_bytes']/1000000000000
filtered.dropna(subset=['ds_replicas_sites'], inplace=True)

# def isLocal(row):
#     return 'True' if row['site_name'] in row['ds_replicas_sites'].split(',') else 'False'
#
# filtered['local'] = filtered.apply(isLocal, axis=1)
#
# local = filtered[filtered['local'] == 'True']
# remote = filtered[filtered['local'] == 'False']
#
total = pd.read_csv('total_volume.csv')
total['total_volume'] = total['total_volume'] / 1000000000000
total.set_index('site_name', inplace=True)

local = filtered[filtered['is_local'] == 1]
remote = filtered[filtered['is_local'] == 2]
from_tape = filtered[filtered['is_local'] == 3]

def agg(df):
    df = df[['datasetname','site_name','site_cloud','ds_bytes']]
    df = df.drop_duplicates()
    grouped = df.groupby(['site_cloud','site_name'])['ds_bytes'].sum().reset_index()
    grouped.set_index('site_name', inplace=True)
    return grouped

result_local = total.join(agg(local)).reset_index()
result_local.rename(columns={"ds_bytes": "local_accessed"}, inplace=True)
#result_local['local%'] = round((result_local['local_accessed']/result_local['total_volume'])*100,1)
result_local.set_index(['site_name','site_cloud','total_volume'],inplace=True)
# result.drop('site_cloud',axis=1,inplace=True)
result_remote = total.join(agg(remote)).reset_index()
result_remote.rename(columns={"ds_bytes": "remote_accessed"}, inplace=True)
#result_remote['remote%'] = round((result_remote['remote_accessed']/result_remote['total_volume'])*100,1)
result_remote.set_index(['site_name','site_cloud','total_volume'],inplace=True)
#result.drop('site_cloud',axis=1,inplace=True)
result_tape = total.join(agg(from_tape)).reset_index()
result_tape.rename(columns={"ds_bytes": "staged_in_from_TAPE"}, inplace=True)
#result_tape['from_TAPE%'] = round((result_tape['staged_in_from_TAPE']/result_tape['total_volume'])*100,1)
result_tape.set_index(['site_name','site_cloud','total_volume'],inplace=True)

result = result_local.join(result_remote).join(result_tape).reset_index()
result['local%'] = round((result['local_accessed']/result['total_volume'])*100,1)
result['remote%'] = round((result['remote_accessed']/result['total_volume'])*100,1)
result['from_TAPE%'] = round((result['staged_in_from_TAPE']/result['total_volume'])*100,1)
result['total_accessed'] = result['local_accessed'] + result['remote_accessed'] + result['staged_in_from_TAPE']
result['total_accessed%'] = round((result['total_accessed']/result['total_volume'])*100,1)

outputfile = 'accessed_data_Feb-2021.xlsx'
result.sort_values(by='total_volume', ascending=False).to_excel(outputfile)

# outputfile = 'total_accessed.xlsx'
# writer = pd.ExcelWriter(outputfile, engine='xlsxwriter')
# total_accessed(local).sort_values(by='total_volume', ascending=False).to_excel(writer,sheet_name='Local')
# total_accessed(remote).sort_values(by='total_volume', ascending=False).to_excel(writer,sheet_name='Remote')



