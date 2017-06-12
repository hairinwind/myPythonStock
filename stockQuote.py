import json
import datetime as dt
import quandl
quandl.ApiConfig.api_key = "ptncathabbVjauEnNMaf"
# import pandas_datareader.data as web
from stockMongo import getAllSymbols, insertQuotes, findSymbol
from getYahooQuotes import getQuotesFromYahoo
from pathlib import Path

def isQuandl(symbol):
    return 'quoteSource' in symbol and symbol['quoteSource'].split("/")[0] == 'quandl'

def getQuotesFromQuandl(symbolDocument):
    quoteSource = symbolDocument['quoteSource']
    symbol = symbolDocument['Symbol']
    datasetCode = quoteSource.split("/")[1]
    df = quandl.get(datasetCode + "/" + symbol)
    df['Symbol'] = symbol
    df=df.rename(columns = {'Adj. Volume':'Adj Volume', 'Adj. Low':'Adj Low', 'Adj. Open':'Adj Open', 'Adj. Close':'Adj Close', 'Adj. High':'Adj High' })
    return df

def getQuotes(symbol, start, end):
    if(isQuandl(symbol)):
        df = getQuotesFromQuandl(symbol)
    else:
        df = getQuotesFromYahoo(symbol['Symbol'])
    return df
    
def convertToJson(df):
    if (len(df) > 0):
        records = df.reset_index()
        if (isinstance(records['Date'][0], dt.date)):
            records['Date'] = records['Date'].dt.strftime('%Y-%m-%d')
        return records.to_dict(orient='records')

def fetchAndStoreQuotes(symbol, start, end):
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
        '''print(len(quotesJson), ' quotes were got...')
        insertQuotes(quotesJson)
        print(len(quotes), ' quotes were inserted...')'''
        csvName = symbol['Symbol']
        if(csvName == 'PRN'):
            csvName += '_quote'
        quotes.to_csv('quotes/{}.csv'.format(csvName), index=False) 

if __name__ == '__main__':        
    start = dt.datetime(2017,1,1)
    end = dt.datetime(2017,6,8)
    symbols = getAllSymbols()    
    '''
    for symbol in symbols:
        if (symbol['Symbol'] == 'PRN'):
            continue
        csvFile = Path("quotes/{}.csv".format(symbol['Symbol']))
        if not csvFile.exists():
            print(symbol['Symbol'])
            fetchAndStoreQuotes(symbol,start, end)
        else:
            print(symbol['Symbol'],'already exists')
    '''
    fetchAndStoreQuotes(findSymbol('AAAP'),start, end)        
