import pandas as pd


def csvReader(ID):
    path = '/Users/joseph/Desktop/GRA/ResearchComponents/Database/dbTesting/testData/multi_meter_datalog_LLC_BLDG_'
    path2 = '_2019-3-24_0-0-0.csv'
    df = pd.read_csv(path + ID + path2,
                       sep=',', header=1, parse_dates=True, infer_datetime_format=True,
                       usecols=['Date', 'coldInFlowRate', 'hotInFlowRate',
                                'hotOutFlowRate', 'hotInTemp', 'hotOutTemp',
                                'coldInTemp'])
    return df

def testResultsUL (testNum):
    results = pd.DataFrame(index=range(1,testNum), columns=['B_csvTime', 'B_dataIngestTime',
                                                            'C_csvTime', 'C_dataIngestTime',
                                                            'D_csvTime', 'D_dataIngestTime',
                                                            'E_csvTime', 'E_dataIngestTime',
                                                            'F_csvTime', 'F_dataIngestTime'
                                                            ])
    return results

def testResultsDL(testNum):
    results = pd.DataFrame(index=range(1,testNum), columns=['query1', 'query2',
                                                            'query3', 'query4'
                                                            ])
    return results