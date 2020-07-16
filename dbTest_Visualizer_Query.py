import pandas as pd
import matplotlib.pyplot as plt
import glob
import os

os.chdir('/Users/augustus/Desktop/GRA/ResearchComponents/dbTesting_results/queries/')

all_files =glob.glob('*.csv')

mem = input('Memory to Viz: ')
#mem = '2048'



# read in query csvs into pandas dataframe organized by db name
for f in all_files:
    if mem in f:
        if "MONGODB" in f:
            mongo = pd.read_csv(f, header=0)
        elif "INFLUXDB" in f:
            influx = pd.read_csv(f, header=0)
        elif "MYSQL" in f:
            mysql = pd.read_csv(f, header=0)
        elif "POSTGRESQL" in f:
            postgresql = pd.read_csv(f, header=0)



# copy all query 1 into 1 pandas dataframe called query 1
q1= pd.DataFrame(influx['query1'])
q1['MongoDB'] = mongo['query1']
q1['MySQL'] = mysql['query1']
q1['PostgreSQL'] = postgresql['query1']

q1.rename(columns={"query1":"InfluxDB"}, inplace=True)

# repeat for each query (2, 3, & 4)
# query2
q2= pd.DataFrame(influx['query2'])
q2['MongoDB'] = mongo['query2']
q2['MySQL'] = mysql['query2']
q2['PostgreSQL'] = postgresql['query2']

q2.rename(columns={"query2":"InfluxDB"}, inplace=True)

# query3
q3= pd.DataFrame(influx['query3'])
q3['MongoDB'] = mongo['query3']
q3['MySQL'] = mysql['query3']
q3['PostgreSQL'] = postgresql['query3']

q3.rename(columns={"query3":"InfluxDB"}, inplace=True)

# query 4
q4= pd.DataFrame(influx['query4'])
q4['MongoDB'] = mongo['query4']
q4['MySQL'] = mysql['query4']
q4['PostgreSQL'] = postgresql['query4']

q4.rename(columns={"query4":"InfluxDB"}, inplace=True)


#  calculate stats for results
dfs = [q1, q2, q3, q4]  # create list of dataframes to iterate through at top level
columns = list(q1)  # create list of df columns to iterate through for stats
names = ['q1', 'q2', 'q3', 'q4'] # create list of df names to assign to results files
x=0  # use manual iterator to assign names to results files



for df in dfs:  # loop through query dfs
    avg = []  # create empty lists for each stat
    stdev = []
    med = []
    for i in columns: # loop over query_# df columns and calculate mean, std dev, and median for each db
        avg.append(df[i].mean())  # append values to list
        stdev.append(df[i].std())
        med.append(df[i].median())

    results = pd.DataFrame({'query':names[x],
                            'DB':columns,
                            'Mean':avg,
                           'Std Dev':stdev,
                           'Median':med
                            })

    results.set_index(['query'], inplace=True)
    results.to_csv('queries_results/'+names[x] + '_' + mem + '_results.csv', )
    x += 1





# plot each query df as boxplot
black_diamond = dict(markerfacecolor='black', marker='D')

gridsize = (4,1)
fig = plt.figure(1, figsize=(12,14), tight_layout=True)


ax1 = plt.subplot2grid(gridsize, (0,0))
ax1 = q1.boxplot(flierprops = black_diamond, showfliers=False)
#ax1.set_xlabel("Database")
ax1.set_ylabel("Q1 \n Time (s)",fontsize = 14)
plt.xticks([])
plt.yticks(fontsize=12)
ax1.set_title('Data Query Tasks for: '+mem+'MB', fontsize =14 )

ax2 = plt.subplot2grid(gridsize, (1,0))
ax2 = q2.boxplot(flierprops = black_diamond, showfliers=False)
#ax2.set_xlabel("Database")
ax2.set_ylabel("Q2 \n Time (s)",fontsize = 14)
plt.xticks([])
plt.yticks(fontsize=12)


ax3 = plt.subplot2grid(gridsize, (2,0))
ax3 = q3.boxplot(flierprops = black_diamond, showfliers=False)
#ax3.set_xlabel("Database")
ax3.set_ylabel("Q3 \n Time (s)",fontsize = 14)
plt.xticks([])
plt.yticks(fontsize=12)


ax4 = plt.subplot2grid(gridsize, (3,0))
ax4 = q4.boxplot(flierprops = black_diamond, showfliers=False)
ax4.set_xlabel("Database", fontsize = 16, weight ='bold')
ax4.set_ylabel("Q4 \n Time (s)", fontsize = 14)
plt.grid(False, axis = 'x')
plt.xticks(fontsize=14)
plt.yticks(fontsize=12)


plt.savefig('queries_results/queries_'+mem+'.png')
plt.show()


# save as png


print('done')