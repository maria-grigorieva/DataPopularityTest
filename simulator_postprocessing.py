import pandas as pd
import numpy as np

df = pd.read_csv('ReplicationAdvisor/mc16_13TeV.DAOD.TOPQ.deriv/tasks_users/joined_weeks.csv',
                 error_bad_lines=False)

df['curr_n_replicas'].fillna(method='backfill', inplace=True)

# df[df['optimal_n_replicas'] <= 0]['optimal_n_replicas'] = df['curr_n_replicas']

grouped = df.groupby('datasetname')


result = []

for k,v in grouped:
    v.sort_values(by=['timestamp'], ascending=False, inplace=True)
    suggested_replicas = v['optimal_n_replicas'].values
    real_replicas = v['curr_n_replicas'].values
    initial_n_replicas = float(real_replicas[0])
    min_n_replicas = v['curr_n_replicas'].min()

    created_replicas_simul = []
    diff = float(suggested_replicas[0])-initial_n_replicas
    created_replicas_simul.append(diff)
    for i in range(1, len(suggested_replicas)-1):
        created_replicas_simul.append(float(suggested_replicas[i+1]) - float(suggested_replicas[i]))
    overall_simul_created = np.sum(created_replicas_simul)
    if overall_simul_created <= 0 or overall_simul_created < min_n_replicas:
        overall_simul_created = min_n_replicas

    created_replicas_real = []
    diff = float(real_replicas[0]) - initial_n_replicas
    created_replicas_real.append(diff)
    for i in range(1, len(real_replicas)-1):
        created_replicas_real.append(float(real_replicas[i+1]) - float(real_replicas[i]))

    result.append({
        'datasetname': k,
        'replicas_created_simul': overall_simul_created,
        'ds_size_GB': v['ds_size_GB'].values[0],
        'total_n_accesses': v['curr_n_accesses'].sum(),
        'total_n_users': v['curr_n_users'].max(),
        'replicas_created_real': v['curr_n_replicas'].max(),
        'datasetAF': v['datasetAF'].mean()
    })
    # overall_simul_created = v['optimal_n_replicas'].values[0]
    # if overall_simul_created <= 0 or overall_simul_created < min_n_replicas:
    #     overall_simul_created = min_n_replicas
    # result.append({
    #     'datasetname': k,
    #     'replicas_created_simul': overall_simul_created,
    #     'ds_size_GB': v['ds_size_GB'].values[0],
    #     'total_n_accesses': v['curr_n_accesses'].sum(),
    #     'replicas_created_real': v['curr_n_replicas'].values[0]
    # })

result_df = pd.DataFrame(result)
result_df['simul_created_volume'] = result_df['ds_size_GB']*result_df['replicas_created_simul']
result_df['real_created_volume'] = result_df['ds_size_GB']*result_df['replicas_created_real']
result_df.to_csv('ReplicationAdvisor/mc16_13TeV.DAOD.TOPQ.deriv/tasks_users/result.csv')
