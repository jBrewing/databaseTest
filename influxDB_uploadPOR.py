import pandas as pd
from influxdb import DataFrameClient
from influxdb import InfluxDBClient
import time

from test2 import testResultsUL


print('Receiving inputs...\n')
# Input parameters.
dbType = input("Input DB type: ").upper()
testType = input("Input test type (upload/dl):  ")
dataLength = input("Input data length: ")
testNum = int(input("Input # of trials: "))

PORStart = "'2019-03-22T12:00:00Z'"
POREnd = "'2019-04-19T12:00:00Z'"

print('\nConnecting to database...')
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')

print('Establishing connection to '+dbType+' database...')
# Connect to destination db
try:
    client_testDB = DataFrameClient(host='192.168.212.133', port=8086)
    print('connection successful!')

except:
    print('connection unsuccessful :(')

client_testDB.switch_database('ciws_por')


testResults = testResultsUL(testNum)
testData = ['B','C','D','E','F']


# Upload data by buildingID
print('testing '+dbType+' db...\n')
for x in testData:
    # Read in CSV file, start timer
    bldgID = "'" + x + "'"
    query = """SELECT * FROM "flow" WHERE "buildingID" =""" + bldgID + """ AND time >= """ + PORStart + """ AND time <= """ + POREnd + """"""

    print('Retrieving data...')
    # Convert returned ResultSet to Pandas dataframe with list
    # and get_points.
    # Set dataframe index as datetime.
    results = client.query(query)
    ls = list(results.get_points(measurement='flow'))
    df = pd.DataFrame(ls)
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)

    # Start timer for influxDB upload
    print('\tuploading '+x+' to '+dbType)
    db_startTime = time.time()
    client_testDB.write_points(df, measurement='flow',
                            field_columns={'coldInFlowRate': df[['coldInFlowRate']],
                                           'hotInFlowRate': df[['hotInFlowRate']],
                                           'hotOutFlowRate': df[['hotOutFlowRate']],
                                           'hotInTemp': df[['hotInTemp']],
                                           'hotOutTemp': df[['hotOutTemp']],
                                           'coldInTemp': df[['coldInTemp']]},
                            tag_columns={'buildingID': df[['buildingID']]}, protocol='line', numeric_precision=10, batch_size=10000)
    db_FinishTime = time.time() - db_startTime
    testResults.at[1, x+'_dataIngestTime'] = db_FinishTime


client.close()

print('Printing results!')
testNum = str(testNum)
testResults.to_csv('/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/'+dbType+'/'
                   ''+dbType+'_'+testType+'_'+testNum+'trials_'+dataLength+'.csv')


print('DONE!')