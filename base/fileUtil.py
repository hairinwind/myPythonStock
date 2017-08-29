import functools
import os
import pickle
import zipfile
from numpy.core.defchararray import startswith


QUOTES_DIR = "d:/quotes/"
QUOTES_ERROR_DIR = QUOTES_DIR + "error/"
QUOTES_SUCCESS_DIR = QUOTES_DIR + "success/"
PREDICTION_REPORT_DIR = QUOTES_DIR + "report/"

MACHINE_LEARNING_PICKLE = "D:/quotes/pickle/"


# parse file name ATI_2017-06-12_2017-06-12.csv, returns ATI 2017-06-12 2017-06-12
def parse(csvFile):
    return csvFile.split('.')[0].split('_')
    
def saveQuotesToCsv(symbol, quotes, start, end):
    if (quotes is not None):
        quotes.to_csv(QUOTES_DIR + '{}_{}_{}.csv'.format(symbol, start, end))
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
    
    
def archiveQuoteFileToZip(dir, fileFullPath):
    file = os.path.basename(fileFullPath)
    symbol = getSymbolFromFileName(file)
    zipFileName = symbol + ".zip"
    if symbol in ['CON', 'NUL', 'PRN']:
        zipFileName = symbol + "_1.zip"
    zf = zipfile.ZipFile(dir + zipFileName, mode='a')
    try:
        zf.write(fileFullPath, os.path.basename(fileFullPath))
        os.remove(fileFullPath)
    finally:
        zf.close()    
    
    
def zipQuoteFiles():
    targetDir = QUOTES_SUCCESS_DIR
    files = os.listdir(QUOTES_DIR) 
    list(map(lambda x: archiveQuoteFileToZip(x[0], x[1]), [(targetDir, QUOTES_DIR + file) for file in files if os.path.isfile(QUOTES_DIR + file)]))
                
    
if __name__ == '__main__':    
    zipQuoteFiles()



