import pandas as pd
from sqlalchemy import create_engine
import time
import mysql.connector
from test2 import csvReader, testResults

dbType = input("Input DB type: ").upper()
testType = input("Input test type (upload/dl):  ")
testNum = int(input("Input # of trials: "))


print('Establishing connection to '+dbType+' database...')
# Create engine object with SQLAlchemy.  Through connection pool and dialect, which describes how
#   to talk to a specific kind of database/DBAPI combo (from SQLAlchemy 1.3 doc).
#   create_engine('dbtype + python library://dbUsername:dbPassword@dbIPaddress:port/dbToQuery')
engine = create_engine('mysql+pymysql://test:foobar123@192.168.212.134:3306/ciws_POR')
#mySQLdb = mysql.connector.connect(
#    host='192.168.212.134',
#    user='test',
#   password='foobar123',
#    database='ciws_POR')
print('Connection established!')



print('Preparing '+dbType+' db query test...')


# Create empty dataframe based on length of test, specified with input, to store test results
testResults = testResults(testNum)

# Query 1:  1 Day of hotInFlowRate and hotInTemp data from one building
start = "'2019-03-23 12:00:00'"
end = "'2019-03-24 12:00:00'"
query1= """SELECT hotInFlowRate, hotInTemp FROM flow WHERE buildingID ='B' AND time BETWEEN """ + start + """ AND """ + end + """"""

# Query 2: 1 week of all flowrates from 3 buildings
start = "'2019-03-31 8:00:00'"
end = "'2019-04-06 20:00:00'"
query2 = """SELECT hotInFlowRate, coldInFlowRate, hotOutFlowRate FROM flow WHERE buildingID = 'C' OR 'E' OR 'D' and time between 
            """+start+""" and """+end+""""""

# Query 3: POR of all variables from one building
start = "'2019-03-22 12:00:00'"
end = "'2019-04-19 12:00:00'"
query3 = """SELECT * FROM flow WHERE buildingID ='F' AND time BETWEEN 
            """+start+""" and """+end+""""""

# Query 4: One week of flowrate, aggregated to the hour, for 2 buildings, with temp converted to F
start = "'2019-03-23 00:00:00'"
end = "'2019-03-31 00:00:00'"
query4 = """SELECT HOUR(time) AS hour, buildingID, AVG(coldInFLowRate) as AvgColdIn, (9.0/5.0)*AVG(coldInTemp)+32 as AvgColdTemp_F 
FROM flow WHERE buildingID = 'E' AND time BETWEEN """+start+""" and """+end+""" 
GROUP BY hour"""

# Loop through test for number of trials specified in input.
print('testing '+dbType+' db...\n')
for i in range(testNum):
    print('Trial #',i)

    print('query #1')
    # Executing query #1
#    query_StartTime = time.time()
#   df = pd.read_sql(query1, con=engine)
#    query1_FinishTime = time.time()-query_StartTime
#    testResults.at[i,'query1'] = query1_FinishTime

    # Executing query #2
    print('query #2')
#    query_StartTime = time.time()
#    df = pd.read_sql(query2, con=engine)
#    query_FinishTime = time.time()-query_StartTime
#    testResults.at[i,'query2'] = query_FinishTime

    # Executing query #3
    print('query #3')
#    query_StartTime = time.time()
#    df = pd.read_sql(query3, con=engine)
#    query_FinishTime = time.time() - query_StartTime
 #   testResults.at[i, 'query3'] = query_FinishTime

    # Executing query #4
    print('query #4')
    query_StartTime = time.time()
    df = pd.read_sql(query4, con=engine)
    query_FinishTime = time.time() - query_StartTime
    testResults.at[i, 'query4'] = query_FinishTime

# Close connection to db
mySQLdb.close()


print('Printing results!')
testNum = str(testNum)
testResults.to_csv('/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/'+dbType+'/'
                   ''+dbType+'_'+testType+'_'+testNum+'trials_.csv')





