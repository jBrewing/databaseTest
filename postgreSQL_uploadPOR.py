import pandas as pd
from influxdb import InfluxDBClient
from sqlalchemy import create_engine
import time
from test2 import testResults


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
# Create engine object with SQLAlchemy.  Through connection pool and dialect, which describes how
#   to talk to a specific kind of database/DBAPI combo (from SQLAlchemy 1.3 doc).
#   create_engine('dbtype + python library://dbUsername:dbPassword@dbIPaddress:port/dbToQuery')
engine = create_engine('postgresql+psycopg2://test:foobar123@192.168.212.133:5432/ciws_por')
print('Connection established!')



print('Preparing '+dbType+' db test...')

# Create empty dataframe based on length of test, specified with input, to store test results
testResults = testResults(testNum)
testData = ['B','C','D','E','F']

# Loop through test for number of trials specified in input.
print('Uploading POR to '+dbType+' db...\n')
for x in testData:
    print('Assembling query for '+x+'...')
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

    print('\tuploading ' + x + ' to ' + dbType)
    # Start timer for SQL upload
    db_startTime = time.time()
    df.to_sql(name='flow', con=engine, if_exists='append',index_label='time')
    db_FinishTime = time.time() - db_startTime
    testResults.at[1, x+'_dataIngestTime'] = db_FinishTime



print('Printing results!')
testNum = str(testNum)
testResults.to_csv('/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/'+dbType+'/'
                   ''+dbType+'_'+testType+'_'+testNum+'trials_'+dataLength+'.csv')





print('done')