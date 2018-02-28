import multiprocessing

from base import fileUtil
from base import stockMongo
from base.stockMongo import findAllActiveSymbols, findSymbol
import datetime as dt
import pandas as pd
import time
from quote.getYahooQuotes import getQuotesFromYahoo 


def getQuotes(symbol, start, end):
    df = getQuotesFromYahoo(symbol['Symbol'], start, end)
    return df
    
def toDict(df):
    if (len(df) > 0):
        records = df.reset_index()
        if (isinstance(records['Date'][0], dt.date)):
            records['Date'] = records['Date'].dt.strftime('%Y-%m-%d')
        return records.to_dict(orient='records')


def retryFetchQuotes(symbol, start, end):
    retryCount = 0
    while retryCount < 5:
        try:
            return getQuotes(symbol, start, end)
        except Exception as e:
            print(type(e))
            print(str(e))
            print('... error to get quotes for ', symbol['Symbol'])
            retryCount += 1
            time.sleep(1)
    return None


def storeQuoteToCsv(symbol, start, end, quotes):
    if (quotes is None):
        print(symbol['Symbol'], 'does not have any quote in the period...')
    else:
        fileUtil.saveQuotesToCsv(symbol['Symbol'], quotes, start, end)

def fetchAndStoreQuotes(symbol, start='1900-01-01', end='2100-12-31'):
    quotes = retryFetchQuotes(symbol, start, end)
    storeQuoteToCsv(symbol, start, end, quotes)
    

def weaveInNextTxDayData(df):
    if len(df) < 2:
        return df
    df = df.sort_values(['Date'])
    df['nextClose'] = df['Close'].shift(-1)
    df = df.apply(pd.to_numeric, errors='ignore')
    df['nextClosePercentage'] = (df['nextClose'] - df['Close']) / df['Close']
    return df
     
 
def getAndSaveNextTxDayData(quotes):
#     df = pd.DataFrame(list(quotes))
    df = weaveInNextTxDayData(quotes)
    if len(df) > 1:
        values = df[['_id', 'nextClose', 'nextClosePercentage']].values
        for value in values:
            if pd.notnull(value[1]) and pd.notnull(value[2]):
                stockMongo.updateQuoteNextClose(value[0], value[1], value[2])
    return df
 
# def initialNextTxDayData(symbol):
#     quotes = stockMongo.findAllQuotesBySymbol(symbol['Symbol'])
#     try:
#         getAndSaveNextTxDayData(quotes)
#     except Exception as e:
#         print(symbol['Symbol'], 'has error......')
#         print(str(e))
# 
# this is the method to weave in the next day close price      
# def initialAllNextTxDayData():
#     symbols = findAllActiveSymbols()
#     with multiprocessing.Pool(multiprocessing.cpu_count() - 1) as p:
#         p.map(initialNextTxDayData, symbols)
        

if __name__ == '__main__':        
    start = '2018-02-09'
    end = '2018-02-09'
       
    '''
    symbols = findAllActiveSymbols()
    for symbol in symbols:
        # csvFile = Path("quotes/{}.csv".format(symbol['Symbol']))
        # if not csvFile.exists():
        print(symbol['Symbol'])
        fetchAndStoreQuotes(symbol, start, end)
    '''
    fetchAndStoreQuotes(findSymbol('SPY'), start, end)        
