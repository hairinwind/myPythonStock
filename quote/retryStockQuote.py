import os
from base import fileUtil
import multiprocessing
from quote.getYahooQuotes import getQuotesFromYahoo

def retryStock(csvFile):
	symbol, start, end = fileUtil.parse(csvFile)
	print('retry', symbol)
	df = getQuotesFromYahoo(symbol, start, end)
	fileUtil.saveQuotesToCsv(symbol, df, start, end)
	fileUtil.removeErrorCsv(csvFile)

if __name__ == '__main__':
    multiprocessing.freeze_support()
    # errorCsvFiles = [x for x in os.listdir(fileUtil.QUOTES_ERROR_DIR) if x.endswith('.csv')]
    errorCsvFiles = os.listdir(fileUtil.QUOTES_ERROR_DIR)
    with multiprocessing.Pool(multiprocessing.cpu_count() - 1) as p:  #multiprocessing.cpu_count() - 1
        p.map(retryStock, errorCsvFiles)
