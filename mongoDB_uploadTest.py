import pandas as pd
from pymongo import MongoClient
import time



def csvReader(ID):
    path = '/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/testData/multi_meter_datalog_LLC_BLDG_'
    path2 = '_2019-3-24_0-0-0.csv'
    df = pd.read_csv(path + ID + path2,
                       sep=',', header=1, parse_dates=True, infer_datetime_format=True,
                       usecols=['Date', 'coldInFlowRate', 'hotInFlowRate',
                                'hotOutFlowRate', 'hotInTemp', 'hotOutTemp',
                                'coldInTemp'])
    return df

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
testResults = pd.DataFrame(index=range(1,testNum), columns=['B_csvTime', 'B_dataIngestTime',
                                                            'C_csvTime', 'C_dataIngestTime',
                                                            'D_csvTime', 'D_dataIngestTime',
                                                            'E_csvTime', 'E_dataIngestTime',
                                                            'F_csvTime', 'F_dataIngestTime'
                                                            ])
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
        sql_startTime = time.time()
        col.insert_many(dictData)
        sql_FinishTime = time.time() - sql_startTime
        testResults.at[i, x+'_dataIngestTime'] = sql_FinishTime

print('Printing results!')
testNum = str(testNum)
testResults.to_csv('/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/'+dbType+'/'
                   ''+dbType+'_'+testType+'_'+testNum+'trials_'+dataLength+'.csv')


print('done')