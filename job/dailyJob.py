import datetime
import functools
import base.fileUtil as fileUtil
from base.parallel import runToAllDone
from base.stockMongo import getAllSymbols, findLatestTwoDaysQuote, savePrediction, findLatestPredictionDate
import base.stockMongo as stockMongo
from machinelearning.machineLearningMode1 import predict, determineResult
import pandas as pd
from quote.loadCsvToMongo import loadAllQuoteFiles
from quote.stockQuote import fetchAndStoreQuotes, getAndSaveNextTxDayData
from multiprocessing import freeze_support

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
            predictObj = {"Symbol":symbol['Symbol'], "date":predictionDate, "prediction":int(predictionResult)}
            savePrediction(predictObj)
    except FileNotFoundError:
        print("machine learning pickle file not found...")
        
def getPredictReportFileName(date, expectPrediction):
    trend = "up"
    if expectPrediction == -1 :
        trend = "down"
    return fileUtil.PREDICTION_REPORT_DIR+"/predict-{}-{}.csv".format(date, trend)      
        

def getPredictDataFrame(date, expectPrediction):
    predict_df = pd.DataFrame(list(stockMongo.findPredictionByDate(date, expectPrediction)))
    accuracy_df = pd.DataFrame(list(stockMongo.findLearnAccuracy()))
    merged_df = pd.merge(predict_df, accuracy_df, on=['Symbol'])
    merged_df = merged_df.apply(pd.to_numeric, errors='ignore')
    merged_df = merged_df.sort_values(['Confidence', 'trainNumber'], ascending=False)
    return merged_df[['Symbol', 'Confidence', 'trainNumber']]

def getAndSavePredictReport(date, expectPrediction):
    df = getPredictDataFrame(date, expectPrediction)
    df.to_csv(getPredictReportFileName(date, expectPrediction), index=False)

def predictReport():
    date = findLatestPredictionDate()
    getAndSavePredictReport(date, 1)
    getAndSavePredictReport(date, -1)
    
def fetchAndStore(symbol):
    start = datetime.datetime.now().strftime("%Y-%m-%d")
    end = datetime.datetime.now().strftime("%Y-%m-%d")
    fetchAndStoreQuotes(symbol, start, end)
    
def now():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
if __name__ == '__main__':      
    freeze_support()
    print(now(), "daily job is started...")
      
    start = datetime.datetime.now().strftime("%Y-%m-%d")
    end = datetime.datetime.now().strftime("%Y-%m-%d")
    
    #check if previous predict exists, if not, do it
    
    symbols = getAllSymbols()
    # fetchAndStore = functools.partial(fetchAndStoreQuotes, start=start, end=end)
    # fetchAndStore = lambda symbol: fetchAndStoreQuotes(symbol, start, end)
    
    runToAllDone(fetchAndStore, [(symbol, ) for symbol in symbols], NUMBER_OF_PROCESSES = 12)
    print(now(), "quote csv files were all downloaded...")
    loadAllQuoteFiles()
    print(now(), 'starting weave in next Tx day data')
    runToAllDone(saveNextTxDayData, [(symbol, ) for symbol in symbols])
    print(now(), 'starting prediction for next Tx day')
    runToAllDone(predictAndSave, [(symbol, ) for symbol in symbols]) # , NUMBER_OF_PROCESSES=1 
        
    print(now(), 'generating predict report')
    predictReport() 
        
    print(now(), "daily job is done...")