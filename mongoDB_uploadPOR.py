import pandas as pd
from pymongo import MongoClient
from influxdb import InfluxDBClient
import time
from test2 import csvReader, testResults


print('Receiving inputs...\n')
# Input parameters.
dbType = input("Input DB type: ").upper()
testType = input("Input test type (upload/dl):  ")
dataLength = input("Input data length: ")
testNum = int(input("Input # of trials: "))

PORStart = "'2019-03-22T12:00:00Z'"
POREnd = "'2019-04-19T12:00:00Z'"

print('establishing connection...')
try:
    client = MongoClient("mongodb://test:foobar123@192.168.212.133/ciws")
    db = client['ciws']
    col = db['flow']
    print('connection successful!')
except:
    print('connection unsuccessful :(')

print('\nConnecting to database...')
client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root',password='foobar123')
client.switch_database('ciws')

print('preparing for '+dbType+' database test...')
testResults = testResults(testNum)
testData = ['B','C','D','E','F']

# Loop through test for number of trials specified in input.
print('testing '+dbType+' db...\n')
for x in testData:
    bldgID = "'" + x + "'"
    query = """SELECT * FROM "flow" WHERE "buildingID" =""" + bldgID + """ AND time >= """ + PORStart + """ AND time <= """ + POREnd + """"""


    print('Retrieving data...')
    # Convert returned ResultSet to Pandas dataframe with list
    # and get_points.
    # Set dataframe index as datetime.
    df_Query = client.query(query)
    ls = list(df_Query.get_points(measurement='flow'))
    df = pd.DataFrame(ls)
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)
    df.columns = df.columns.str.lower()

    print('\tconverting to dictionary...')
    dictData = df.to_dict(orient='records')

    # Start timer for mongoDB upload
    print('\tuploading '+x+' to '+dbType)
    db_startTime = time.time()
    col.insert_many(dictData)
    db_FinishTime = time.time() - db_startTime
    testResults.at[1, x+'_dataIngestTime'] = db_FinishTime

client.close()

print('Printing results!')
testNum = str(testNum)
testResults.to_csv('/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/'+dbType+'/'
                   ''+dbType+'_'+testType+'_'+testNum+'trials_'+dataLength+'.csv')


print('done')