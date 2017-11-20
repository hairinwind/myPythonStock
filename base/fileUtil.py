import os
import pickle
import zipfile


QUOTES_DIR = "H:/quotes/"
QUOTES_ERROR_DIR = QUOTES_DIR + "error/"
QUOTES_SUCCESS_DIR = QUOTES_DIR + "success/"
PREDICTION_REPORT_DIR = QUOTES_DIR + "report/"

MACHINE_LEARNING_PICKLE = "H:/quotes/pickle/"


def getSymbolCsvFileName(symbol, start, end):
    return '{}_{}_{}.csv'.format(symbol, start, end)


def saveQuotesToCsv(symbol, quotes, start, end):
    if (quotes is not None):
        quotes.to_csv(QUOTES_DIR + getSymbolCsvFileName(symbol, start, end))
    else:
        print(symbol, 'quotes is None')


def removeErrorCsv(csvFile):
    os.remove(QUOTES_ERROR_DIR + csvFile)

    
def pickleIt(fileName, data):
    with (open(MACHINE_LEARNING_PICKLE + fileName, "wb")) as f:
        pickle.dump(data, f)

        
def loadPickle(fileName):
    if not os.path.isfile(MACHINE_LEARNING_PICKLE + fileName):
        print("pickle file {} does not exist".format(fileName))
        return
    
    with (open(MACHINE_LEARNING_PICKLE + fileName, "rb")) as f:
        return pickle.load(f)


def getSymbolFromFileName(file):
    return file.split('.')[0].split('_')[0]
    
    
def symbolCannotBeCreatedAsFileOnWindows(symbol):
    return symbol.upper() in ['CON', 'NUL', 'PRN']  

    

def archiveToZipFile(dir, fileFullPath, zipFileName):
    zf = zipfile.ZipFile(dir + zipFileName, mode='a')
    try:
        zf.write(fileFullPath, os.path.basename(fileFullPath))
        os.remove(fileFullPath)
    finally:
        zf.close()


def archiveQuoteFileToZip(dir, fileFullPath):
    file = os.path.basename(fileFullPath)
    symbol = getSymbolFromFileName(file)
    zipFileName = symbol + ".zip"
    if symbolCannotBeCreatedAsFileOnWindows(symbol) :
        zipFileName = symbol + "_1.zip"
    archiveToZipFile(dir, fileFullPath, zipFileName)    
    




