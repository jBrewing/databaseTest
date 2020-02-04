import pandas as pd
from influxdb import InfluxDBClient
import time

from test2 import testResultsDL

print('Receiving inputs...\n')
# Input parameters.
dbType = input("Input DB type: ").upper()
testType = input("Input test type (upload/dl):  ")
testNum = int(input("Input # of trials: "))


print('Establishing connection to '+dbType+' database...')
# Connect to destination db
try:
    client = InfluxDBClient(host='192.168.212.135', port=8086)
    client.switch_database('ciws_por')
    print('Connection established!')

except:
    print('connection unsuccessful :(')


print('Preparing ' + dbType + ' db query test...')
# Create empty dataframe based on length of test, specified with input, to store test results
testResults = testResultsDL(testNum)


#query = """SELECT * FROM "flow" WHERE "buildingID" =""" + bldgID + """ AND time >= """ + PORStart + """ AND time <= """ + POREnd + """"""

print('Building queries...')
# Query 1:  1 Day of hotInFlowRate and hotInTemp data from one building
start = "'2019-03-23 12:00:00'"
end = "'2019-03-24 12:00:00'"
query1= """SELECT "hotInFlowRate", "hotInTemp" 
FROM "flow" 
WHERE "buildingID" ='B' AND time >= """ + start + """ AND time <= """ + end + """"""

# Query 2: 1 week of all flowrates from 3 buildings
start = "'2019-03-31 8:00:00'"
end = "'2019-04-06 20:00:00'"
query2 = """SELECT "hotInFlowRate", "coldInFlowRate", "hotOutFlowRate" 
FROM "flow" 
WHERE "buildingID" = 'C' OR "buildingID"='E' OR "buildingID"='D' and time >=
            """+start+""" and time <="""+end+""""""

# Query 3: POR of all variables from one building
start = "'2019-03-22 12:00:00'"
end = "'2019-04-19 12:00:00'"
query3 = """SELECT * 
FROM "flow" 
WHERE "buildingID" ='F' AND time >=
            """+start+""" and time <="""+end+""""""

# Query 4: One week of flowrate, aggregated to the hour, for 2 buildings, with temp converted to F
start = "'2019-03-23 00:00:00'"
end = "'2019-03-31 00:00:00'"
query4 = """SELECT MAX("coldInFlowRate") , (9.0/5.0)*MEAN("coldInTemp")+32
FROM "flow" 
WHERE "buildingID" = 'E' AND time >= """+start+""" and time <="""+end+""" 
GROUP BY time(1h)"""


print('testing '+dbType+' db...\n')
for i in range(testNum):
    print('Trial #', i)

    print('\tquery #1')
    # Query 1:  1 Day of hotInFlowRate and hotInTemp data from one building
    query_StartTime = time.time()
    df_query = client.query(query1)
    ls = list(df_query.get_points(measurement='flow'))
    df = pd.DataFrame(ls)
    query_FinishTime = time.time() - query_StartTime
    testResults.at[i, 'query1'] = query_FinishTime

    # Query 2: 1 week of all flowrates from 3 buildings
    print('\tquery #2')
    query_StartTime = time.time()
    results = client.query(query2)
    ls = list(results.get_points(measurement='flow'))
    df = pd.DataFrame(ls)
    query_FinishTime = time.time() - query_StartTime
    testResults.at[i, 'query2'] = query_FinishTime

    # Query 3: POR of all variables from one building
    print('\tquery #3')
    query_StartTime = time.time()
    results = client.query(query3)
    ls = list(results.get_points(measurement='flow'))
    df = pd.DataFrame(ls)
    query_FinishTime = time.time() - query_StartTime
    testResults.at[i, 'query4'] = query_FinishTime

    # Query 4: One week of flowrate, aggregated to the hour with max value, for 1 building, with temp converted to F
    print('\tquery #4')
    query_StartTime = time.time()
    results = client.query(query4)
    ls = list(results.get_points(measurement='flow'))
    df = pd.DataFrame(ls)
    query_FinishTime = time.time() - query_StartTime
    testResults.at[i, 'query4'] = query_FinishTime

print('Printing results!')
testNum = str(testNum)
testResults.to_csv('/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/'+dbType+'/'
                   ''+dbType+'_'+testType+'_'+testNum+'trials_.csv')

print('Done!')