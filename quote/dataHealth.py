import functools

from base import dateUtil
from base import parallel
from base import stockMongo
from base.parallel import runToAllDone
import datetime as dt
from machinelearning import machineLearning
from machinelearning import machineLearningUtil
import pandas as pd


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
            print('the nextClose/nextClosePercentage is Null... ')
            print(quotes)
            print('...')
#             stockMongo.updateQuoteNextClose(quotes[1]["_id"], quotes[1]['nextClose'], quotes[1]['nextClosePercentage'])

def isPreviousNextCloseUpdated(date):
    symbols = stockMongo.findAllActiveSymbols(); 
    start = dt.datetime.now()
    
    func = functools.partial(checkAndUpdate, date=date)
        
    parallel.runToAllDone(func, [(symbol,) for symbol in symbols], NUMBER_OF_PROCESSES=8)
    end = dt.datetime.now()
    print('time consumed:', (end - start).seconds)


def isPredictionInserted(date):
    result = stockMongo.countPredictionByDateMode(date)
    df = pd.DataFrame(list(result))
    checkPredictionCount = lambda machineLearningMode : machineLearningUtil.checkPredictionCount(df, machineLearningMode)
    list(map(checkPredictionCount, machineLearning.machineLearningModes))


def isPreviousPredictionResultUpdated(date):
    previousTxDate = stockMongo.findPreviousTxDate(date)
    result = stockMongo.countPredictionHasIsCorrect(previousTxDate)
    df = pd.DataFrame(list(result))
    checkPreviousPrediction = lambda machineLearningMode : machineLearningUtil.checkPredictionCount(df, machineLearningMode, text="isCorrect")
    list(map(checkPreviousPrediction, machineLearning.machineLearningModes))


def dailyCheck(date):
    if isQuoteLoaded(date) == False :
        print('quote count is less than ', MINIMUN_QUOTE_EXPECTED)
     
    isPreviousNextCloseUpdated(date)
    isPredictionInserted(date)
    isPreviousPredictionResultUpdated(date)
    
    print('data load is good...')


def deleteDuplicatedPrediction(mode):
    duplicatedPredictions = list(stockMongo.findDuplicatedPrediction(mode));

    dateSymbol_df = pd.DataFrame([item['_id'] for item in duplicatedPredictions])
    '''
              Date Symbol
    0   2017-08-10   SFBC
    1   2017-08-10   SVBI
    2   2017-08-10  SENEB
    '''
    
#     k: v for k, v in df.groupby('Region')
    dateSymbols = {date:list(group_df['Symbol']) for date, group_df in dateSymbol_df.groupby('Date')}
#     for date, symbol_df in dateSymbol_df.groupby('Date'):
#         dateSymbols[date] = list(symbol_df['Symbol'])
    '''
    dateSymbols
    {
    '2017-06-12': ['JIVE'], 
    '2017-06-15': ['BNCN'], 
    '2017-06-16': ['CNCO', 'SPAN', 'GNVC'],
    ...
    }
    '''
    
    parallel.runToAllDone(stockMongo.deletePredictionBySymbolAndDate, [(mode, symbols, date) for date, symbols in dateSymbols.items()], NUMBER_OF_PROCESSES=8)
#     for date, symbols in dateSymbols.items() :
#         stockMongo.deletePredictionBySymbolAndDate(mode, symbols, date)


def deleteQuoteHasNoNextClose(quoteCount):
    symbolDate = quoteCount['_id']
    quotes = stockMongo.findQuotesBySymbolDate(symbolDate['Symbol'], symbolDate['Date'])
    noNextCloseQuotes = list(filter(lambda quote: 'nextClose' not in quote, quotes))
    toDeleteQuotes = None;
    if len(noNextCloseQuotes) == 0 or len(quotes) == len(noNextCloseQuotes): 
        toDeleteQuotes = quotes[1:];  # keep one document if non document has 'nextClose' 
    else: 
        toDeleteQuotes = noNextCloseQuotes;
    result = stockMongo.deleteByIds([quote['_id'] for quote in toDeleteQuotes])
    print(result)
    


def deleteDuplicatedQuote(startDate, endDate):
    # check data month by month
#     symbols = stockMongo.findAllActiveSymbols()
#     startDate = '2017-04-01'
#     endDate = '2017-07-01'
    print(startDate, endDate)
    duplicatedQuotes = stockMongo.findDuplicatedQuotes(startDate, endDate)
    runToAllDone(deleteQuoteHasNoNextClose, [(quoteCount,) for quoteCount in duplicatedQuotes]) 
    

def getDateRanges(startDate, endDate, interval):
    result = [];
    startDate = currentEndDate = dateUtil.toDate(startDate)
    endDate = dateUtil.toDate(endDate)
    while (currentEndDate < endDate):
        startDate = currentEndDate
        currentEndDate = dateUtil.addDays(startDate, interval)
        result.append((startDate, currentEndDate))
    return result

def findAndDeleteDuplicatedQuote():
    dataRanges = getDateRanges('2016-08-20', '2017-04-01', 100)    
    runToAllDone(deleteDuplicatedQuote, dataRanges)
    
if __name__ == '__main__':
#     date = '2017-07-06'
#     dailyCheck(date)
#     deleteDuplicatedPrediction("mode3")
    deleteDuplicatedQuote('2017-05-19', '2017-08-19')
#     findAndDeleteDuplicatedQuote()
