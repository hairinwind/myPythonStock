import datetime
import functools
import multiprocessing
import base.fileUtil as fileUtil
from base.stockMongo import getAllSymbols, findLatestTwoDaysQuote, savePrediction, findLatestPredictionDate
import base.stockMongo as stockMongo
from machinelearning.machineLearningMode1 import predict
import pandas as pd
from quote.loadCsvToMongo import loadAllQuoteFiles
from quote.stockQuote import fetchAndStoreQuotes, getAndSaveNextTxDayData


def saveNextTxDayData(symbol):
    quotes = findLatestTwoDaysQuote(symbol['Symbol'])
    getAndSaveNextTxDayData(quotes)
    # update the predict 
    
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

if __name__ == '__main__':      
    print(datetime.datetime.now().strftime("%Y-%m-%d"), "daily job is started...")
      
    start = datetime.datetime.now().strftime("%Y-%m-%d")
    end = datetime.datetime.now().strftime("%Y-%m-%d")
    
    '''fetchAndStore = functools.partial(fetchAndStoreQuotes, start=start, end=end)
    
    symbols = getAllSymbols()
    
    multiprocessing.freeze_support()
    with multiprocessing.Pool(multiprocessing.cpu_count() -1) as p:  
        p.map(fetchAndStore, symbols)
        
    loadAllQuoteFiles()
        
    print('starting weave in next Tx day data')
    with multiprocessing.Pool(multiprocessing.cpu_count() -1) as p:  
        p.map(saveNextTxDayData, symbols)    
        
    print('starting prediction for next Tx day')
    with multiprocessing.Pool(multiprocessing.cpu_count() -1) as p:  
        p.map(predictAndSave, symbols)'''
        
    predictReport()
                
    print(datetime.datetime.now().strftime("%Y-%m-%d"), "daily job is done...")