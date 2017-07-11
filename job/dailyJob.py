import datetime
import functools
from multiprocessing import freeze_support
import time
import base.fileUtil as fileUtil
from base.parallel import runToAllDone
from base.stockMongo import getAllActiveSymbols, findLatestTwoDaysQuote, savePrediction, findLatestPredictionDate
import base.stockMongo as stockMongo
from machinelearning.machineLearningMode1 import predict, determineResult
from machinelearning.machineLearningMode2 import predict as predict2
import pandas as pd
from quote.loadCsvToMongo import loadAllQuoteFiles
from quote.stockQuote import fetchAndStoreQuotes, getAndSaveNextTxDayData


def saveNextTxDayData(symbol):
    quotes = findLatestTwoDaysQuote(symbol['Symbol'])
    df = getAndSaveNextTxDayData(quotes)
    if len(df) > 1 :
        date = df['Date'].values[0]
        nextClosePercentage = df['nextClosePercentage'].values[0]
        result = determineResult(nextClosePercentage)
        stockMongo.updatePredictionIsCorrect(symbol['Symbol'], date, result)
    
def predictAndSave(symbol):
    try:
        print(symbol['Symbol'])
        predictions = predict(symbol['Symbol'])
        
        if predictions is None:
            return
        
        for prediction in predictions:
            print(prediction[0])
            print(prediction[1])
            predictionResult = prediction[0]
            predictionDate = prediction[1]
            predictObj = {"Symbol":symbol['Symbol'], "Date":predictionDate, "Prediction":int(predictionResult), "Mode":"mode1"}
            savePrediction(predictObj)
    except FileNotFoundError:
        print("machine learning pickle file not found...")
        
def predictAndSave2(symbol):
    try:
        print(symbol['Symbol'])
        predictions = predict2(symbol['Symbol'])
        
        if predictions is None:
            return
        
        for prediction in predictions:
            print(prediction[0])
            print(prediction[1])
            predictionResult = prediction[0]
            predictionDate = prediction[1]
            predictObj = {"Symbol":symbol['Symbol'], "Date":predictionDate, "Prediction":int(predictionResult), "Mode":"mode2"}
            savePrediction(predictObj)
    except FileNotFoundError:
        print("machine learning pickle file not found...")
        
def getPredictReportFileName(mode, date, expectPrediction):
    trend = "up"
    if expectPrediction == -1 :
        trend = "down"
    return fileUtil.PREDICTION_REPORT_DIR+"/predict-{}-{}-{}.csv".format(date, mode, trend)      
        

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
      
    start = datetime.datetime.now().strftime("%Y-%m-%d")
    end = datetime.datetime.now().strftime("%Y-%m-%d")
    
    #check if previous predict exists, if not, do it
    
    symbols = list(getAllActiveSymbols())
    fetchAndStore = functools.partial(fetchAndStoreQuotes, start=start, end=end)
    # fetchAndStore = lambda symbol: fetchAndStoreQuotes(symbol, start, end)
    
    # runToAllDone(fetchAndStore, [(symbol, ) for symbol in symbols], NUMBER_OF_PROCESSES = 12)

    print(now(), "quote csv files were all downloaded...")
    loadAllQuoteFiles()
     
    time.sleep(300)
     
    print(now(), "save next tx data...")
    runToAllDone(saveNextTxDayData, [(symbol, ) for symbol in symbols])
     
    print(now(), 'starting prediction for next Tx day')
    runToAllDone(predictAndSave, [(symbol, ) for symbol in symbols]) # , NUMBER_OF_PROCESSES=1 
    
    runToAllDone(predictAndSave2, [(symbol, ) for symbol in symbols]) # , NUMBER_OF_PROCESSES=1 
          
    print(now(), 'generating predict report')
    predictReport() 
    
    # verify
        
    print(now(), "daily job is done...")
    
    
if __name__ == '__main__':      
    freeze_support()
    runDailyJob()