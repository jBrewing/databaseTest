import pandas as pd
from pymongo import MongoClient
import time
from test2 import csvReader, testResultsUL


print('Receiving inputs...\n')
# Input parameters.
dbType = input("Input DB type: ").upper()
testType = input("Input test type (upload/dl):  ")
dataLength = input("Input data length: ")
testNum = int(input("Input # of trials: "))

print('establishing connection...')

try:
    client = MongoClient("mongodb://test:foobar123@192.168.212.133/ciws")
    db = client['ciws']
    col = db['flow']
    print('connection successful!')
except:
    print('connection unsuccessful :(')

print('preparing for '+dbType+' database test...')
testResults = testResultsUL(testNum)
testData = ['B','C','D','E','F']

# Loop through test for number of trials specified in input.
print('testing '+dbType+' db...\n')
for i in range(testNum):
    print('Trial #',i)

    delete = col.delete_many({})


    for x in testData:
        # Read in CSV file, start timer
        print('\treading '+x+' csv')
        csv_StartTime = time.time()
        df = csvReader(x)
        csv_FinishTime = time.time()-csv_StartTime
        testResults.at[i,x+'_csvTime'] = csv_FinishTime

        print('\tconverting to dictionary...')
        dictData = df.to_dict(orient='records')

        # Start timer for mongoDB upload
        print('\tuploading '+x+' to '+dbType)
        db_startTime = time.time()
        col.insert_many(dictData)
        db_FinishTime = time.time() - db_startTime
        testResults.at[i, x+'_dataIngestTime'] = db_FinishTime

client.close()

print('Printing results!')
testNum = str(testNum)
testResults.to_csv('/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/'+dbType+'/'
                   ''+dbType+'_'+testType+'_'+testNum+'trials_'+dataLength+'.csv')


print('done')