import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.font_manager import FontProperties
import numpy
import glob
import os




os.chdir('/Users/augustus/Desktop/GRA/ResearchComponents/dbTesting_results/storage/')
#all_files =glob.glob(mem+'MB/*.csv')


dbs = ['INFLUXDB', 'MYSQL', 'POSTGRESQL', 'MONGODB']
size = ['1 BLDG', '5 BLDGS']


storage = pd.read_csv('storage_results_final.csv')
storage.set_index('DB', inplace=True, drop=True)


# plot results
N = 2
ind = numpy.arange(N)

fig, ax = plt.subplots()
fig = plt.figure(1, figsize=(12,14), tight_layout=True)

width = 0.15

p1 = ax.bar(x=ind-width/2-width,height=storage.loc['INFLUXDB'],
            width=width, capsize=5, label = 'InfluxDB')
p2 = ax.bar(x=ind-width/2,height=storage.loc['MYSQL'],
            width=width,capsize=5, label = 'MySQL')
p3 = ax.bar(x=ind+width/2, height=storage.loc['MONGODB'],
            width=width,capsize=5, label = 'MongoDB')
p4 = ax.bar(x=ind+width/2+width, height=storage.loc['POSTGRESQL'],
            width=width,capsize=5, label = 'PostgreSQL')

#ax.set_yscale('log')
ax.grid(which='both',axis='y', color='grey', linewidth='1', alpha=0.5)
ax.set_xlabel('Number of Buildings', fontsize=14)
ax.set_ylabel('Data Size (MB)', fontsize=14)
ax.set_xticks(ind)
ax.set_xticklabels(size)
ax.set_ylim(0, 1200)
ax.legend()
ax.set_title('Database storage comparison', fontsize =14)

def autolabel(ps):
    for p in ps:
        height = p.get_height()
        ax.annotate('{}'.format(height),
                    xy=(p.get_x() + p.get_width()/2, height),
                    xytext=(0,3),
                    textcoords="offset points",
                    ha='center', va='bottom')
autolabel(p1)
autolabel(p2)
autolabel(p3)
autolabel(p4)


os.chdir('/Users/augustus/Desktop/GRA/ResearchComponents/dbTesting_results/storage/')
plt.savefig('storage_results.png')


plt.show()








print('done')