import multiprocessing

from base.parallel import runToAllDone
import base.stockMongo as stockMongo
import datetime as dt
import pandas as pd


# from machinelearning.machineLearningMode1 import initialMachineLearning
# def learn():
#     symbols = pd.DataFrame(list(stockMongo.findAllActiveSymbols()))['Symbol'].values      
#     with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
#         p.map(initialMachineLearning, symbols)

def getPassedDate(days):
    return dt.datetime.now() - dt.timedelta(days=days)

def checkVolumne(symbol):
    startDate = getPassedDate(120)
    symbols = stockMongo.findSymbolsVolumesLessThan(startDate, 100000)
    print(len(symbols))


def checkSymbolVolume():
    startDate = getPassedDate(120).strftime("%Y-%m-%d")
    symbols = pd.DataFrame(list(stockMongo.findSymbolsVolumesLessThan(startDate, 100000)))['_id'].values
    print(len(symbols))
    print(symbols)
    runToAllDone(stockMongo.addSymbolStatus, [(symbol, "lowVolume") for symbol in symbols])
    
    
if __name__ == '__main__': 
#     learn()
    checkSymbolVolume()
