import datetime
from unittest import mock

import numpy as np
import pandas as pd
from quote import stockQuote


def getSampleDataFrame():
    date = ['2017-06-20', '2017-06-21', '2017-06-23']
    Close = [1, 2, 3]
    return pd.DataFrame(data={'Date':date, 'Close':Close}, index=range(3))

def getSampleDataFrame1():
    date = ['2017-06-21', '2017-06-23', '2017-06-20']
    Close = [1, 2, 3]
    return pd.DataFrame(data={'Date':date, 'Close':Close}, index=range(3))
      
 
def test_weaveInNextTxDayData():
    df = getSampleDataFrame1()
    print(df)
    df1 = stockQuote.weaveInNextTxDayData(df)
    print(df1)
    assert 'nextClose' in df1 
    assert 'nextClosePercentage' in df1
    assert df1.loc[2, 'nextClose'] == 1
    assert df1.loc[0, 'nextClose'] == 2
    assert np.isnan(df1.loc[1, 'nextClose'])
    assert df1.loc[2, 'nextClosePercentage'] == (df.loc[0, 'Close'] - df.loc[2, 'Close']) * 1.0 / df.loc[2, 'Close']
    assert df1.loc[0, 'nextClosePercentage'] == (df.loc[1, 'Close'] - df.loc[0, 'Close']) * 1.0 / df.loc[0, 'Close']
    assert np.isnan(df1.loc[1, 'nextClosePercentage']) 
     
    df1 = stockQuote.weaveInNextTxDayData(df[:1])
    assert 'nextClose' not in df1 
    assert 'nextClosePercentage' not in df1
    

def strptime(str):
    return datetime.datetime.strptime(str, '%Y-%m-%d')

def test_toDict():
    df = getSampleDataFrame()
    dict = stockQuote.toDict(df)
    expectedDict = [{'index': 0, 'Close': 1, 'Date': '2017-06-20'}, {'index': 1, 'Close': 2, 'Date': '2017-06-21'}, {'index': 2, 'Close': 3, 'Date': '2017-06-23'}]
    assert dict == expectedDict
    
    df['Date'] = df['Date'].apply(strptime)
    print(type(df['Date'].values[0]))
    dict = stockQuote.toDict(df)
    assert dict == expectedDict

@mock.patch('quote.stockQuote.getQuotes')    
def test_retryFetchQuotes(mocked_getQuotes):
    symbol = {"Symbol":'KO'}
    start = '2017-06-01'
    end = '2017-06-10'
    mocked_getQuotes.return_value = getSampleDataFrame()
    stockQuote.retryFetchQuotes(symbol, start, end)
    mocked_getQuotes.assert_called_once_with(symbol, start, end)

# def my_side_effect():
#     raise Exception("Test")
 
@mock.patch('quote.stockQuote.getQuotes')    
def test_retryFetchQuotesWithErrors(mocked_getQuotes):
    symbol = {"Symbol":'KO'}
    start = '2017-06-01'
    end = '2017-06-10'
    mocked_getQuotes.side_effect = mock.Mock(side_effect=Exception('Test'))
    stockQuote.retryFetchQuotes(symbol, start, end)
    mocked_getQuotes.assert_called_with(symbol, start, end)
    assert mocked_getQuotes.call_count == 5

@mock.patch('base.fileUtil.saveQuotesToCsv')    
def test_storeQuoteToCsv(mocked_saveQuotesToCsv):
    symbol = {"Symbol":'KO'}
    start = '2017-06-01'
    end = '2017-06-10'
    quotes = None;
    stockQuote.storeQuoteToCsv(symbol, start, end, quotes)
    assert mocked_saveQuotesToCsv.call_count == 0
    
    quotes = getSampleDataFrame();
    stockQuote.storeQuoteToCsv(symbol, start, end, quotes)
    mocked_saveQuotesToCsv.assert_called_once_with(symbol['Symbol'], quotes, start, end)


@mock.patch('quote.stockQuote.storeQuoteToCsv')    
@mock.patch('quote.stockQuote.retryFetchQuotes')
def test_fetchAndStoreQuotes(mocked_retryFetchQuotes, mocked_storeQuoteToCsv):  # 
    symbol = {"Symbol":'KO'}
    start = '2017-06-01'
    end = '2017-06-10'
    df = getSampleDataFrame()
    mocked_retryFetchQuotes.return_value = df 
    stockQuote.fetchAndStoreQuotes(symbol, start, end)  
    mocked_retryFetchQuotes.assert_called_once_with(symbol, start, end)
    mocked_storeQuoteToCsv.assert_called_once_with(symbol, start, end, df)
    

