import datetime
import functools
from multiprocessing import freeze_support
import time

import base.fileUtil as fileUtil
from base.parallel import runToAllDone
from base.stockMongo import findAllActiveSymbols, savePrediction, findLatestPredictionDate
import base.stockMongo as stockMongo
from machinelearning import machineLearning
from machinelearning import machineLearningRunner
import pandas as pd
from quote import dataHealth
from quote.loadCsvToMongo import loadAllQuoteFiles
from quote.stockQuote import fetchAndStoreQuotes, getAndSaveNextTxDayData


machineLearningModes = machineLearning.machineLearningModes
# machineLearningModes = [machineLearningMode3()]


def updateMachineLearingPredictionResult(symbol, df, machineLearingMode):
    date = df['Date'].values[0]
    nextClosePercentage = df['nextClosePercentage'].values[0]
    result = machineLearingMode.determineResult(nextClosePercentage)
    mode = machineLearingMode.MODE
    stockMongo.updatePredictionIsCorrect(symbol, date, mode, result)

def saveNextTxDayData(quote):
    quotes = stockMongo.findLatestQuotesBeforeDate(quote['Symbol'], quote['Date'], 2)
    quotes = pd.DataFrame(list(quotes))
    df = getAndSaveNextTxDayData(quotes)
    if len(df) > 1 :
        for machineLearningMode in machineLearningModes:
            updateMachineLearingPredictionResult(quote['Symbol'], df, machineLearningMode)
    
def predictAndSaveForOneMode(machineLearingMode, date, symbol):
    try:
        print(symbol['Symbol'])
        predictions = machineLearningRunner.predict(machineLearingMode, date, symbol['Symbol'])
        
        if predictions is None:
            return
        
        for prediction in predictions:
            print(prediction[0])
            print(prediction[1])
            predictionResult = prediction[0]
            predictionDate = prediction[1]
            predictObj = {"Symbol":symbol['Symbol'], "Date":predictionDate, "Prediction":int(predictionResult), "Mode":machineLearingMode.MODE}
            print('save prediciton object...')
            print(predictObj)
            savePrediction(predictObj)
    except Exception as e:
        print("error happened when predictAndSaveForOneMode...")
        print(str(e))
        
def predictAndSave(symbol, date):   
    for machineLearingMode in machineLearningModes:
        predictAndSaveForOneMode(machineLearingMode, date, symbol)
    
def getPredictReportFileName(mode, date, expectPrediction):
    trend = "up"
    if expectPrediction == -1 :
        trend = "down"
    return fileUtil.PREDICTION_REPORT_DIR + "/predict-{}-{}-{}.csv".format(date, mode, trend)      
        

def getPredictDataFrame(mode, date, expectPrediction):
    predict_df = pd.DataFrame(list(stockMongo.findPredictionByDate(mode, date, expectPrediction)))
    if predict_df is None or len(predict_df) == 0 :
        return None
    accuracy_df = pd.DataFrame(list(stockMongo.findLearnAccuracy(mode)))
    merged_df = pd.merge(predict_df, accuracy_df, on=['Symbol'])
    merged_df = merged_df.apply(pd.to_numeric, errors='ignore')
    merged_df = merged_df.sort_values(['Confidence', 'trainNumber'], ascending=False)
    return merged_df[['Symbol', 'Confidence', 'trainNumber']]

def getAndSavePredictReport(mode, date, expectPrediction):
    df = getPredictDataFrame(mode, date, expectPrediction)
    if df is not None :
        df.to_csv(getPredictReportFileName(mode, date, expectPrediction), index=False)

def predictReport():
    date = findLatestPredictionDate("mode1")
    getAndSavePredictReport("mode1", date, 1)
    getAndSavePredictReport("mode1", date, -1)
    
    date = findLatestPredictionDate("mode2")
    getAndSavePredictReport("mode2", date, 1)
    getAndSavePredictReport("mode2", date, -1)
    
# def fetchAndStore(symbol):
#     start = datetime.datetime.now().strftime("%Y-%m-%d")
#     end = datetime.datetime.now().strftime("%Y-%m-%d")
#     fetchAndStoreQuotes(symbol, start, end)

def fetch(symbol, start='1900-01-01', end='2100-12-31'):
    print('symbol', symbol)
    print('start', start)
    print('end', end)
    
def now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def runDailyJob():
    print(now(), "daily job is started...")
      
#     start = datetime.datetime.now().strftime("%Y-%m-%d")
#     end = datetime.datetime.now().strftime("%Y-%m-%d")
    start = '2017-08-25'
    end = '2017-08-25'
    
    # check if previous predict exists, if not, do it
    
    symbols = list(findAllActiveSymbols())
    fetchAndStore = functools.partial(fetchAndStoreQuotes, start=start, end=end)
    # fetchAndStore = lambda symbol: fetchAndStoreQuotes(symbol, start, end)
        
    runToAllDone(fetchAndStore, [(symbol,) for symbol in symbols], NUMBER_OF_PROCESSES=12)
         
    print(now(), "quote csv files were all downloaded...")
    loadAllQuoteFiles()
             
    time.sleep(60)
             
    print(now(), "save next tx data...")
    quotes = stockMongo.findQuotesByPeriod(start, end)
    runToAllDone(saveNextTxDayData, [(quote,) for quote in quotes])
          
    print(now(), 'starting prediction for next Tx day')
    runToAllDone(predictAndSave, [(symbol, start) for symbol in symbols])  # , NUMBER_OF_PROCESSES=1 
                 
    print(now(), 'generating predict report')
    predictReport() 
        
    # verify
    for date in pd.date_range(datetime.datetime.strptime(start, "%Y-%m-%d"), datetime.datetime.strptime(end, "%Y-%m-%d")):
        print('check', date)
        dataHealth.dailyCheck(date.strftime("%Y-%m-%d"))
        
    print(now(), "daily job is done...")
    
    
if __name__ == '__main__':      
    freeze_support()
    runDailyJob()
