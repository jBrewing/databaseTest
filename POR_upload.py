import pandas as pd
from influxdb import DataFrameClient,InfluxDBClient
import psycopg2
from pymongo import MongoClient
from sqlalchemy import create_engine
import mysql.connector

from test2 import testResults
import time


print('Receiving inputs...\n')
# Input parameters.
PORStart = "'2019-03-22T12:00:00Z'"
POREnd = "'2019-04-19T12:00:00Z'"


dbType = input("Input DB type: ").upper()
testType = input("Input test type (upload/dl):  ")
dataLength = input("Input data length: ")
testNum = int(input("Input # of trials: "))



if 'influxdb' in dbType:
    client = DataFrameClient(host='192.168.212.133', port=8086)
    client.switch_database('ciws_POR')

elif 'mysql' in dbType:
    engine = create_engine('mysql+pymysql://test:foobar123@192.168.212.134:3306/ciws_POR')
#    mySQLdb = mysql.connector.connect(
#        host='192.168.212.134',
#        user='test',
#        password='foobar123',
#        database='ciws_POR')

elif 'postgresql' in dbType:
    engine = create_engine('postgresql+psycopg2://test:foobar123@192.168.212.133:5432/ciws_POR')
#    myPGSQLdb = psycopg2.connect(
#        host='192.168.212.133',
#        user='test',
#        password='foobar123',
#        database='ciws_POR')

elif 'mongodb' in dbType:
    client = MongoClient("mongodb://test:foobar123@192.168.212.133/ciws_POR")
    db = client['ciws_POR']
    col = db['flow']

else:
    print('dbType not in test bank.')


print('Preparing db test...')
testResults = testResults(testNum)
testData = ['B','C','D','E','F']

for x in testData:
    print('\nConnecting to database...')
    client = InfluxDBClient(host='odm2equipment.uwrl.usu.edu', port=8086, username='root', password='foobar123')
    client.switch_database('ciws')

    print('Retrieving data for POR upload...')
    bldgID = "'" + x + "'"
    query = """SELECT * FROM "flow" WHERE "buildingID" =""" + bldgID + """ AND time >= """ + PORStart + """ AND time <= """ + POREnd + """"""
    df_Query = client.query(query)
    ls = list(df.get_points(measurement='flow'))
    df = pd.DataFrame(ls)
    df['time'] = pd.to_datetime(df['time'])
    df.set_index('time', inplace=True)
    print('data retrieved')






testDBs = ['mysql','postgresql', 'mongodb', 'influxdb']

for i in testDBs:
    dbType = x
    if 'influxdb' in dbType:
        # Start timer for influxDB upload
        print('\tuploading ' + x + ' to ' + dbType)
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
        testResults.at[i, x + '_dataIngestTime'] = db_FinishTime

    elif 'mysql' in dbType:
        print('\tuploading ' + x + ' to ' + dbType)
        db_startTime = time.time()
        df.to_sql(name='flow', con=engine, if_exists='append')
        db_FinishTime = time.time() - db_startTime
        testResults.at[i, x + '_dataIngestTime'] = db_FinishTime

    elif 'postgresql' in dbType:
        print('\tuploading ' + x + ' to ' + dbType)
        sql_startTime = time.time()
        df.to_sql(name='flow', con=engine, if_exists='append', chunksize=10000)
        sql_FinishTime = time.time() - sql_startTime
        testResults.at[i, x + '_dataIngestTime'] = sql_FinishTime

    elif 'mongodb' in dbType:
        #convert records to dict for insertion into Mongo
        dictData = df.to_dict(orient='records')

        # Start timer for mongoDB upload
        print('\tuploading ' + x + ' to ' + dbType)
        db_startTime = time.time()
        col.insert_many(dictData)
        db_FinishTime = time.time() - db_startTime
        testResults.at[i, x + '_dataIngestTime'] = db_FinishTime





