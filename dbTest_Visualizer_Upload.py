import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import numpy
import glob
import os



mem = input('Memory to Viz: ')
os.chdir('/Users/augustus/Desktop/GRA/ResearchComponents/dbTesting_results/uploads/'+mem+'MB/')
all_files =glob.glob(mem+'MB/*.csv')

#mem = '2048'
#period = 'w'


dbs = ['INFLUXDB', 'MYSQL', 'POSTGRESQL', 'MONGODB']
periods = ['Day', 'Week', 'Month']
results = pd.DataFrame(index = dbs, columns = periods)
error = pd.DataFrame(index = dbs, columns=periods)
d = 'D_dataIngestTime'
b = 'B_dataIngestTime'


for db in dbs:
    files = glob.glob(db+'*.csv')
    i = 0
    for f in files:
        if '_d_' in f:
            df = pd.read_csv(f, header = 0, usecols=[b])
            results.at[db,'Day'] = df[b].mean()
            error.at[db, 'Day'] = df[b].std()
        elif '_w_' in f:
            df = pd.read_csv(f, header=0, usecols=[b])
            results.at[db, 'Week'] = df[b].mean()
            error.at[db, 'Week'] = df[b].std()
        else:
            df = pd.read_csv(f, header=0, usecols=[b])
            results.at[db, 'Month'] = df[b].mean()
            error.at[db, 'Month'] = df[b].std()
    i+=1


# plot results
N = 3
ind = numpy.arange(N)

fig, ax = plt.subplots()
fig = plt.figure(1, figsize=(12,14), tight_layout=True)

width = 0.2

p1 = ax.bar(x=ind-width/2-width,height=results.loc['INFLUXDB'],
            yerr=error.loc['INFLUXDB'], width=width, capsize=5, label = 'InfluxDB')
p2 = ax.bar(x=ind-width/2,height=results.loc['MYSQL'],
            yerr=error.loc['MYSQL'], width=width,capsize=5, label = 'MySQL')
p3 = ax.bar(x=ind+width/2, height=results.loc['MONGODB'],
            yerr=error.loc['MONGODB'], width=width,capsize=5, label = 'MongoDB')
p4 = ax.bar(x=ind+width/2+width, height=results.loc['POSTGRESQL'],
            yerr=error.loc['POSTGRESQL'], width=width,capsize=5, label = 'PostgreSQL')

ax.set_yscale('log')
ax.grid(which='both',axis='y', color='grey', linewidth='1', alpha=0.5)
ax.set_xlabel('Data Period', fontsize=14)
ax.set_ylabel('Time (s)', fontsize=14)
ax.set_xticks(ind)
ax.set_xticklabels(periods)
ax.set_ylim(0.1, 1500)
ax.legend()
ax.set_title('Data Ingestion for: '+mem+'MB', fontsize =14)

os.chdir('/Users/augustus/Desktop/GRA/ResearchComponents/dbTesting_results/uploads/')
results.to_csv('uploads_results/'+mem+'_ingestion_results.csv')
error.to_csv('uploads_results/'+mem+'_ingestion_results_error.csv')
plt.savefig('uploads_results/uploads'+mem+'.png')


plt.show()


print('done')