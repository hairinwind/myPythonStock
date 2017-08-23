import numpy as np
import pandas as pd
from quote import stockData

def getSampleDataFrame():
    date = ['2017-06-21', '2017-06-23', '2017-06-20']
    Close = [1,2,3]
    return pd.DataFrame(data = {'Date':date, 'Close':Close}, index=range(3))
    

def test_weaveInNextTxDayData():
    df = getSampleDataFrame()
    print(df)
    df1 = stockData.weaveInNextTxDayData(df)
    print(df1)
    assert 'nextClose' in df1 
    assert 'nextClosePercentage' in df1
    assert df1.loc[2, 'nextClose'] == 1
    assert df1.loc[0, 'nextClose'] == 2
    assert np.isnan(df1.loc[1, 'nextClose'])
    assert df1.loc[2, 'nextClosePercentage'] == (df.loc[0, 'Close'] - df.loc[2, 'Close'])*1.0 / df.loc[2, 'Close']
    assert df1.loc[0, 'nextClosePercentage'] == (df.loc[1, 'Close'] - df.loc[0, 'Close'])*1.0 / df.loc[0, 'Close']
    assert np.isnan(df1.loc[1, 'nextClosePercentage']) 
    
    df1=stockData.weaveInNextTxDayData(df[:1])
    assert 'nextClose' not in df1 
    assert 'nextClosePercentage' not in df1
    
    