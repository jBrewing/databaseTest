import pandas as pd
from influxdb import DataFrameClient
import time

from test2 import csvReader, testResults


print('Receiving inputs...\n')
# Input parameters.
dbType = input("Input DB type: ").upper()
testType = input("Input test type (upload/dl):  ")
dataLength = input("Input data length: ")
testNum = int(input("Input # of trials: "))

measurement='flow'

try:
    client = DataFrameClient(host='192.168.212.133', port=8086)
    print('connection successful!')

except:
    print('connection unsuccessful :(')

client.switch_database('ciws')
query = 'DROP MEASUREMENT "flow"'

testResults = testResults(testNum)
testData = ['B','C','D','E','F']






# Loop through test for number of trials specified in input.
print('testing '+dbType+' db...\n')
for i in range(testNum):
    print('Trial #',i)

    client.query(query)

    for x in testData:
        # Read in CSV file, start timer
        print('\treading '+x+' csv')
        csv_StartTime = time.time()
        df = csvReader(x)
        csv_FinishTime = time.time()-csv_StartTime
        testResults.at[i,x+'_csvTime'] = csv_FinishTime
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        df['buildingID'] = x

        # Start timer for influxDB upload
        print('\tuploading '+x+' to '+dbType)
        db_startTime = time.time()
        client.write_points(df, measurement='flow',
                            field_columns={'coldInFlowRate': df[['coldInFlowRate']],
                                           'hotInFlowRate': df[['hotInFlowRate']],
                                           'hotOutFlowRate': df[['hotOutFlowRate']],
                                           'hotInTemp': df[['hotInTemp']],
                                           'hotOutTemp': df[['hotOutTemp']],
                                           'coldInTemp': df[['coldInTemp']]},
                            tag_columns={'buildingID': df[['buildingID']]}, protocol='line', numeric_precision=10)
        db_FinishTime = time.time() - db_startTime
        testResults.at[i, x+'_dataIngestTime'] = db_FinishTime


client.close()

print('Printing results!')
testNum = str(testNum)
testResults.to_csv('/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/'+dbType+'/'
                   ''+dbType+'_'+testType+'_'+testNum+'trials_'+dataLength+'.csv')


print('DONE!')