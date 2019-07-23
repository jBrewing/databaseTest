import pandas as pd
from sqlalchemy import create_engine
import time
import psycopg2
from test2 import csvReader, testResults


print('Receiving inputs...\n')
# Input parameters.
dbType = input("Input DB type: ").upper()
testType = input("Input test type (upload/dl):  ")
dataLength = input("Input data length: ")
testNum = int(input("Input # of trials: "))


print('Establishing connection to '+dbType+' database...')
# Create engine object with SQLAlchemy.  Through connection pool and dialect, which describes how
#   to talk to a specific kind of database/DBAPI combo (from SQLAlchemy 1.3 doc).
#   create_engine('dbtype + python library://dbUsername:dbPassword@dbIPaddress:port/dbToQuery')
engine = create_engine('postgresql+psycopg2://test:foobar123@192.168.212.133:5432/ciws')
myPGSQLdb = psycopg2.connect(
    host='192.168.212.133',
    user='test',
    password='foobar123',
    database='ciws'
)
print('Connection established!')



print('Preparing '+dbType+' db test...')

# Create empty dataframe based on length of test, specified with input, to store test results
testResults = testResults(testNum)
testData = ['B','C','D','E','F']

# Loop through test for number of trials specified in input.
print('testing '+dbType+' db...\n')
for i in range(testNum):
    print('Trial #',i)

    mycursor = myPGSQLdb.cursor()
    sql = "DROP TABLE IF EXISTS flow"
    mycursor.execute(sql)
    myPGSQLdb.commit()


    for x in testData:
        # Read in CSV file, start timer
        print('\treading '+x+' csv')
        csv_StartTime = time.time()
        df = csvReader(x)
        csv_FinishTime = time.time()-csv_StartTime
        testResults.at[i,x+'_csvTime'] = csv_FinishTime

        # Start timer for SQL upload
        print('\tuploading '+x+' to sql')
        sql_startTime = time.time()
        df.to_sql(name='flow', con=engine, if_exists='append')
        sql_FinishTime = time.time() - sql_startTime
        testResults.at[i, x+'_dataIngestTime'] = sql_FinishTime

# Close connection to db
myPGSQLdb.close()

print('Printing results!')
testNum = str(testNum)
testResults.to_csv('/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/'+dbType+'/'
                   ''+dbType+'_'+testType+'_'+testNum+'trials_'+dataLength+'.csv')





print('done')