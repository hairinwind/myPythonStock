import multiprocessing
import functools
import datetime
from base.stockMongo import getAllSymbols, findLatestTwoDaysQuote, savePrediction
from quote.stockQuote import fetchAndStoreQuotes, getAndSaveNextTxDayData
from machinelearning.machineLearningMode1 import predict
from quote.loadCsvToMongo import loadAllQuoteFiles

def saveNextTxDayData(symbol):
    quotes = findLatestTwoDaysQuote(symbol['Symbol'])
    getAndSaveNextTxDayData(quotes)
    
def predictAndSave(symbol):
    try:
        print(symbol['Symbol'])
        prediction, date = predict(symbol['Symbol'])
        
        if prediction is None:
            return
        
        print(prediction[0])
        print(date)
        predictObj = {"Symbol":symbol['Symbol'], "date":date, "prediction":int(prediction[0])}
        savePrediction(predictObj)
    except FileNotFoundError:
        print("machine learning pickle file not found...")

if __name__ == '__main__':      
    print(datetime.datetime.now().strftime("%Y-%m-%d"), "daily job is started...")
      
    start = datetime.datetime.now().strftime("%Y-%m-%d")
    end = datetime.datetime.now().strftime("%Y-%m-%d")
    
    fetchAndStore = functools.partial(fetchAndStoreQuotes, start=start, end=end)
    
    symbols = getAllSymbols()
    
    '''multiprocessing.freeze_support()
    with multiprocessing.Pool(multiprocessing.cpu_count() -1) as p:  
        p.map(fetchAndStore, symbols)'''
        
    '''loadAllQuoteFiles()
        
    print('starting weave in next Tx day data')
    with multiprocessing.Pool(multiprocessing.cpu_count() -1) as p:  
        p.map(saveNextTxDayData, symbols)'''
        
    print('starting prediction for next Tx day')
    with multiprocessing.Pool(multiprocessing.cpu_count() -1) as p:  
        p.map(predictAndSave, symbols)
        
    # update prediction result    
        
    print(datetime.datetime.now().strftime("%Y-%m-%d"), "daily job is done...")