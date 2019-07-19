import pandas as pd
from sqlalchemy import create_engine
import time
import mysql.connector


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


print('Establishing connection to '+dbType+' database...')
# Create engine object with SQLAlchemy.  Through connection pool and dialect, which describes how
#   to talk to a specific kind of database/DBAPI combo (from SQLAlchemy 1.3 doc).
#   create_engine('dbtype + python library://dbUsername:dbPassword@dbIPaddress:port/dbToQuery')
engine = create_engine('mysql+pymysql://test:foobar123@192.168.212.133:3306/ciws')
mySQLdb = mysql.connector.connect(
    host='192.168.212.133',
    user='test',
    password='foobar123',
    database='ciws'
)
print('Connection established!')



print('Preparing '+dbType+' db test...')

# Create empty dataframe based on length of test, specified with input, to store test results
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

    mycursor = mySQLdb.cursor()
    sql = "DROP TABLE flow"
    mycursor.execute(sql)


    for x in testData:
        # Read in CSV file, start timer
        csv_StartTime = time.time()
        df = csvReader(x)
        csv_FinishTime = time.time()-csv_StartTime
        testResults.at[i,x+'_csvTime'] = csv_FinishTime

        # Start timer for SQL upload
        sql_startTime = time.time()
        df.to_sql(name='flow', con=engine, if_exists='append')
        sql_FinishTime = time.time() - sql_startTime
        testResults.at[i, x+'_dataIngestTime'] = sql_FinishTime

# Close connection to db
mySQLdb.close()


print('Printing results!')
testNum = str(testNum)
testResults.to_csv('/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/'+dbType+'/'
                   ''+dbType+'_'+testType+'_'+testNum+'trials_'+dataLength+'.csv')





print('done')