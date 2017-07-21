from base import stockMongo
from base import parallel
from machinelearning import machineLearning
import functools
import pandas as pd
import numpy as np
import datetime as dt
    
MINIMUN_QUOTE_EXPECTED = 5000

# check if quote is loaded into db
def isQuoteLoaded(date):
    quoteCount = stockMongo.quotesCountByDate(date)
    return quoteCount is not None and quoteCount > MINIMUN_QUOTE_EXPECTED

def checkAndUpdate(symbol, date=dt.datetime.now()):
    quotes = list(stockMongo.findLatestQuotesBeforeDate(symbol['Symbol'], date, 2))
    if len(quotes) > 1 : 
        if "nextClose" not in quotes[1] or "nextClosePercentage" not in quotes[1] or quotes[1]["nextClose"] is None or quotes[1]["nextClosePercentage"] is None :
            quotes[1]['nextClose'] = quotes[0]['Close']
            quotes[1]['nextClosePercentage'] = (quotes[0]['Close'] - quotes[1]['Close']) / quotes[1]['Close']
            stockMongo.updateQuoteNextClose(quotes[1]["_id"], quotes[1]['nextClose'],  quotes[1]['nextClosePercentage'])

def isPreviousNextCloseUpdated(date):
    symbols = stockMongo.getAllActiveSymbols(); 
    start = dt.datetime.now()
    
    func = functools.partial(checkAndUpdate, date=date)
        
    parallel.runToAllDone(func, [(symbol, ) for symbol in symbols], NUMBER_OF_PROCESSES = 8)
    end = dt.datetime.now()
    print('time consumed:', (end-start).seconds)


def isPredictionInserted(date):
    result = stockMongo.countPredictionByDateMode(date)
    df = pd.DataFrame(list(result))
    checkPredictionCount = lambda machineLearningMode : machineLearning.checkPredictionCount(df, machineLearningMode)
    list(map(checkPredictionCount, machineLearning.machineLearningModes))


def isPreviousPredictionResultUpdated(date):
    previousTxDate = stockMongo.findPreviousTxDate(date)
    result = stockMongo.countPredictionHasIsCorrect(previousTxDate)
    df = pd.DataFrame(list(result))
    checkPreviousPrediction = lambda machineLearningMode : machineLearning.checkPredictionCount(df, machineLearningMode, text="isCorrect")
    list(map(checkPreviousPrediction, machineLearning.machineLearningModes))


def dailyCheck(date):
    if isQuoteLoaded(date) == False :
        print('quote count is less than ', MINIMUN_QUOTE_EXPECTED)
     
    isPreviousNextCloseUpdated(date)
    isPredictionInserted(date)
    isPreviousPredictionResultUpdated(date)
    
    print('data load is good...')

# this method is to eliminate bad data
def removeDuplicate():
    duplicates = stockMongo.findDuplicatedQuotes()
    df = pd.DataFrame(list(duplicates))
    for item in df['_id'].values:
        symbol = item['Symbol']
        date = item['Date']
        quotes = stockMongo.findQuotesBySymbolDate(symbol, date)
        quotes_df = pd.DataFrame(list(quotes))
        
        # if nextClose or nextClosePercentage is null, put in the values from previous
        # nextCloseNotNull_df = quotes_df.loc[lambda df: pd.notnull(df.nextClose),:]
        # nextCloseNull_df = quotes_df.loc[lambda df: pd.isnull(df.nextClose)]
        for i in range(1, len(quotes_df)):
            if pd.isnull(quotes_df.loc[i, 'nextClose']) : 
                quotes_df.loc[i, 'nextClose'] = quotes_df.loc[i-1, 'nextClose']
                quotes_df.loc[i, 'nextClosePercentage'] = quotes_df.loc[i-1, 'nextClosePercentage']
                stockMongo.updateQuoteNextClose(quotes_df.loc[i, '_id'], quotes_df.loc[i-1, 'nextClose'], quotes_df.loc[i-1, 'nextClosePercentage'])
        
        quotes = stockMongo.findQuotesBySymbolDate(symbol, date)
        quotes_df = pd.DataFrame(list(quotes))
        null_df = quotes_df.loc[lambda df: pd.isnull(df.Open) | pd.isnull(df.Close) | pd.isnull(df.Volume) | df.Open == 0,:]
                
        for id in null_df['_id'].values:
            stockMongo.deleteById(id)
            
            
    duplicates = stockMongo.findDuplicatedQuotes()
    df = pd.DataFrame(list(duplicates))
    for item in df['_id'].values:
        symbol = item['Symbol']
        date = item['Date']
        quotes = stockMongo.findQuotesBySymbolDate(symbol, date)
        quotes_df = pd.DataFrame(list(quotes))
        for i in range(0, len(quotes_df)-1) :
            stockMongo.deleteById(quotes_df.loc[i,'_id'])
    
    # both have full data, delete the first one, keep the last one
    duplicates = stockMongo.findDuplicatedQuotes()
    df = pd.DataFrame(list(duplicates))
    print(df)
    
if __name__ == '__main__':
    date = '2017-07-06'
    dailyCheck(date)