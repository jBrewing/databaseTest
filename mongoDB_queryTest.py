import pandas as pd
from pymongo import MongoClient
import time
from datetime import datetime

from test2 import testResultsDL

print('Receiving inputs...\n')
# Input parameters.
dbType = input("Input DB type: ").upper()
testType = input("Input test type (upload/dl):  ")
testNum = int(input("Input # of trials: "))


print('establishing connection...')
try:
    client = MongoClient("mongodb://test:foobar123@192.168.212.133/ciws_por")
    db = client.ciws_por
    col = db.flow
    print('Connection established!')
except:
    print('connection unsuccessful :(')


print('Preparing '+dbType+' db query test...')
# Create empty dataframe based on length of test, specified with input, to store test results
testResults = testResultsDL(testNum)

#print('Building queries...')

# Loop through test for number of trials specified in input.
print('testing '+dbType+' db...\n')
for i in range(testNum):
    print('Trial #', i)

    print('\tquery #1')
    # Query 1:  1 Day of hotInFlowRate and hotInTemp data from one building
    start = datetime(2019, 3,23, 12,0,0)
    end = datetime(2019, 3,24,12,0,0)
    query_StartTime = time.time()
    mydoc = col.find({"time":{'$gte':start, '$lt':end}, "buildingid":"B"}, {"hotinflowrate":1,"hotintemp":1,"_id":0})
    df = pd.DataFrame(list(mydoc))
    query_FinishTime = time.time() - query_StartTime
    testResults.at[i, 'query1'] = query_FinishTime


    print('\tquery #2')
    # Query 2: 1 week of all flowrates from 3 buildings
    start = datetime(2019,3,31,8,0,0)
    end = datetime(2019,4,6,20,0,0)
    query_StartTime = time.time()
    mydoc = col.find({"time":{'$gte':start,'$lt': end}, "buildingid":{'$in':["C", "E", "D"]}},
                    {"buildingid":1, "hotinflowrate":1, "hotoutflowrate":1, "coldinflowrate": 1, "_id":0})
    df = pd.DataFrame(list(mydoc))
    query_FinishTime = time.time() - query_StartTime
    testResults.at[i, 'query2'] = query_FinishTime


    print('\tquery #3')
    # Query 3: POR of all variables from one building
    start = "'2019-03-22 12:00:00'"
    start = datetime(2019,3,22,12,0,0)
    end = datetime(2019,4,19,12,0,0)
    query_StartTime = time.time()
    mydoc = col.find({"time":{'$gte':start,'$lt': end},"buildingid":"F"},
                    {"_id":0})
    df = pd.DataFrame(list(mydoc))
    query_FinishTime = time.time() - query_StartTime
    testResults.at[i, 'query3'] = query_FinishTime


    print('\tquery #4')
    # Query 4: One week of flowrate, aggregated to the hour, for 1 buildings, with temp converted to F
    start = datetime(2019,3,23,0,0,0)
    end = datetime(2019,3,31,0,0,0)
    query_StartTime = time.time()
    mydoc = col.aggregate([
        {'$match': {
            "time": {'$gte': start, '$lt': end},
            "buildingid": "E"}
        },
        {'$project': {
            "Temp_F":{'$add':[32,{'$multiply':[1.8,"$coldintemp"]}]},
            "coldinflowrate":1,
            "time":1,
            "_id":0}
        },
        {'$group':{
            "_id":{
                "hour":{'$hour':"$time"},
                },
            "AvgTemp_F":{'$avg':"$Temp_F"},
            "MaxColdFlow":{'$max': "$coldinflowrate"}}
        },
        {'$sort':{
            "_id.hour":1}
            }

        ])
    df = pd.DataFrame(list(mydoc))
    query_FinishTime = time.time() - query_StartTime
    testResults.at[i, 'query4'] = query_FinishTime



print('Printing results!')
testNum = str(testNum)
testResults.to_csv('/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/'+dbType+'/'
                   ''+dbType+'_'+testType+'_'+testNum+'trials_.csv')

print('Done!')























