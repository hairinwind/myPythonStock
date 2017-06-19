import os
import pickle

QUOTES_DIR = "d:/quotes/"
QUOTES_ERROR_DIR = QUOTES_DIR + "error/"
QUOTES_SUCCESS_DIR = QUOTES_DIR + "success/"

MACHINE_LEARNING_PICKLE = "D:/quotes/pickle/"


# parse file name ATI_2017-06-12_2017-06-12.csv, returns ATI 2017-06-12 2017-06-12
def parse(csvFile):
    return csvFile.split('.')[0].split('_')
    
def saveQuotesToCsv(symbol, quotes, start, end):
    if (quotes is not None):
        quotes.to_csv(QUOTES_DIR+'{}_{}_{}.csv'.format(symbol, start, end))
    else:
        print(symbol, 'quotes is None')

def removeErrorCsv(csvFile):
    os.remove(QUOTES_ERROR_DIR+csvFile)
    
def pickleIt(fileName, data):
    with (open(MACHINE_LEARNING_PICKLE + fileName, "wb")) as f:
        pickle.dump(data, f)
        
def loadPickle(fileName):
    with (open(MACHINE_LEARNING_PICKLE + fileName, "rb")) as f:
        return pickle.load(f)