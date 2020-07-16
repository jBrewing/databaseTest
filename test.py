import pandas as pd
from influxdb import InfluxDBClient
import time
import matplotlib.pyplot as plt

from test2 import testResultsDL

print('Receiving inputs...\n')
# Input parameters.
#dbType = input("Input DB type: ").upper()
#testType = input("Input test type (upload/dl):  ")
testNum = int(input("Input # of trials: "))
chunksize = int(input("Input chunksize: "))

#print('Establishing connection to '+dbType+' database...')
# Connect to destination db
try:
    client = InfluxDBClient(host='192.168.212.135', port=8086)
    client.switch_database('ciws_por')
    print('Connection established!')

except:
    print('connection unsuccessful :(')


#rint('Preparing ' + dbType + ' db query test...')
# Create empty dataframe based on length of test, specified with input, to store test results
testResults = testResultsDL(testNum)

# Query 3: POR of all variables from one building
start = "'2019-03-22 12:00:00'"
end = "'2019-04-19 12:00:00'"
query3 = """SELECT * 
FROM "flow" 
WHERE "buildingID" ='F' AND time >=
            """+start+""" and time <="""+end+""""""


#print('testing '+dbType+' db...\n')
for i in range(testNum):
    print('Trial #', i)


    # Query 3: POR of all variables from one building
    #print('\tquery #3')
    query_StartTime = time.time()
    results = client.query(query3)
    ls = list(results.get_points(measurement='flow'))
    df = pd.DataFrame(ls)
    query_FinishTime = time.time() - query_StartTime
    testResults.at[i, 'query3'] = query_FinishTime

    print(query_FinishTime)

print('Printing results!')
chunksize = str(chunksize)
testResults.to_csv('/Users/augustus/Desktop/_query3_'+chunksize+'.csv')


print('done')