import pandas as pd
import glob

def merge(path):
    all_files = glob.glob(path)
    li = []
    for filename in all_files:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    return pd.concat(li, axis=0, ignore_index=True)


df = merge('ReplicationAdvisor/mc16_13TeV.DAOD.TOPQ.deriv/tasks_users/*.csv')
df.to_csv('ReplicationAdvisor/mc16_13TeV.DAOD.TOPQ.deriv/tasks_users/joined_weeks.csv')

# df1 = pd.read_csv('RawData/FebMarch2021.csv')
#
# result = pd.concat([df,df1])
# result.to_csv('RawData/FebMarch2021+1week.csv')
#
# df.to_csv('FebMarch2021.csv')