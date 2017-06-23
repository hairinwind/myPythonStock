import multiprocessing

import quandl

from base import fileUtil
from base import stockMongo
from base.stockMongo import getAllSymbols, findSymbol
import datetime as dt
import pandas as pd
import numpy as np
from quote import stockData
from quote.getYahooQuotes import getQuotesFromYahoo 


quandl.ApiConfig.api_key = "ptncathabbVjauEnNMaf"
# import pandas_datareader.data as web


def isQuandl(symbol):
    return 'quoteSource' in symbol and symbol['quoteSource'].split("/")[0] == 'quandl'

def getQuotesFromQuandl(symbolDocument, start, end):
    quoteSource = symbolDocument['quoteSource']
    symbol = symbolDocument['Symbol']
    datasetCode = quoteSource.split("/")[1]
    if (start is not None and end is not None):
        df = quandl.get(datasetCode + "/" + symbol, start_date=start, end_date=end)
    else:
        df = quandl.get(datasetCode + "/" + symbol)
    df['Symbol'] = symbol
    df=df.rename(columns = {'Adj. Volume':'Adj Volume', 'Adj. Low':'Adj Low', 'Adj. Open':'Adj Open', 'Adj. Close':'Adj Close', 'Adj. High':'Adj High' })
    return df

def getQuotes(symbol, start, end):
    '''if(isQuandl(symbol)):
        df = getQuotesFromQuandl(symbol, start, end)
    else:'''
    df = getQuotesFromYahoo(symbol['Symbol'], start, end)
    return df
    
def toDict(df):
    if (len(df) > 0):
        records = df.reset_index()
        if (isinstance(records['Date'][0], dt.date)):
            records['Date'] = records['Date'].dt.strftime('%Y-%m-%d')
        return records.to_dict(orient='records')

def fetchAndStoreQuotes(symbol, start='1900-01-01', end='2100-12-31'):
    retryCount = 0
    quotes = None
    while (retryCount < 5):
        try:
            quotes = getQuotes(symbol, start, end)
            break
        except Exception as e: 
            print(type(e))
            print(str(e))
            print('... error to get quotes for ', symbol['Symbol'])
            retryCount += 1

    # quotesJson = convertToJson(quotes)
    if (quotes is None):
        print(symbol['Symbol'], 'does not have any quote in the period...')
    else:
        fileUtil.saveQuotesToCsv(symbol['Symbol'], quotes, start, end)

def getAndSaveNextTxDayData(quotes):
    df = pd.DataFrame(list(quotes))
    df = stockData.weaveInNextTxDayData(df)
    if len(df) > 1:
        values = df[['_id', 'nextClose', 'nextClosePercentage']].values
        for value in values:
            if pd.notnull(value[1]) and pd.notnull(value[2]):
                stockMongo.updateQuoteNextClose(value[0], value[1], value[2])
    return df

def initialNextTxDayData(symbol):
    quotes = stockMongo.findAllQuotesBySymbol(symbol['Symbol'])
    try:
        getAndSaveNextTxDayData(quotes)
    except Exception as e:
        print(symbol['Symbol'], 'has error......')
        print(str(e))

# this is the method to weave in the next day close price      
def initialAllNextTxDayData():
    symbols = getAllSymbols()
    with multiprocessing.Pool(multiprocessing.cpu_count() - 1) as p:
        p.map(initialNextTxDayData, symbols)
        

if __name__ == '__main__':        
    start = '1960-01-01'
    end = '2017-06-21'
       
    '''
    symbols = getAllSymbols()
    for symbol in symbols:
        # csvFile = Path("quotes/{}.csv".format(symbol['Symbol']))
        # if not csvFile.exists():
        print(symbol['Symbol'])
        fetchAndStoreQuotes(symbol, start, end)
    '''
    fetchAndStoreQuotes(findSymbol('INF'), start, end)        
    
    #initialAllNextTxDayData()
