from base import stockMongo
import pandas as pd
from base import fileUtil
from sklearn import svm, neighbors
from sklearn.model_selection import train_test_split
from sklearn.ensemble import VotingClassifier, RandomForestClassifier
from base.parallel import runToAllDone
import multiprocessing
import numpy as np
from machinelearning import machineLearingUtil
import functools

# mode2 only uses the quotes for last n years, default is 3
# weave in the data of ^DJI, ^NYA, ^GSPC, ^IXIC 

MODE = 'mode2'
trainingDataColumns = ['Open', 'High', 'Low', 'Close', 'Volume', '3_mean', '7_mean', '3_mean_volume', '7_mean_volume', 'closeOpenPercentage']
MINIMUN_MACHINE_LEARNING_NUMBERS = 100
TEST_SIZE = 0.4
QUOTES_NUMBER = 600 # 3 years approximately  600 quotes
THRESHOLD1=0.2
THRESHOLD2=-0.2

determineResult = functools.partial(machineLearingUtil.determineResult, THRESHOLD1, THRESHOLD2)

def getInitialData(symbol):
    quotes = stockMongo.findLatestQuotes(symbol, QUOTES_NUMBER)
    df = pd.DataFrame(list(quotes))
    
    if len(df) == 0:
        stockMongo.updateSymbolStatus(symbol, stockMongo.SYMBOL_INACTIVE);
        return;
    
    # replace string 'null' value with np.nan
    df = df.replace('null', np.nan)
    # drop the rows if OHLCV is na
    df.dropna(how='any', subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)
    return df

#extract the dataframe to the featuerset for machine learning

def addValue(df):
    df = df.apply(pd.to_numeric, errors='ignore')
    df['3_mean'] = df['Close'].rolling(window=3).mean()
    df['7_mean'] = df['Close'].rolling(window=7).mean()
    df['3_mean_volume'] = df['Volume'].rolling(window=3).mean()
    df['7_mean_volume'] = df['Volume'].rolling(window=7).mean()
    df['closeOpenPercentage'] = (df['Close'] - df['Open']) / df['Close']
    return df

def getLastRowDate(df):
    return df['Date'].values[-1]

def getFirstRowDate(df):
    return df['Date'].values[0]

def removeUnusedColumnsAndRows(df):
    # remove unused columns
    df.drop(['Adj Low', 'Adj High', 'Adj Open', 'Adj Volume', 'Ex-Dividend', 'Split Ratio', 'index', 'level_0'], axis=1, inplace=True, errors='ignore')
    #remove rows contains nan
    df = df.dropna(how='any')
    return df

def extract_featureset(df):
    if 'nextClosePercentage' not in df.columns :
        return None, None, df
    
    df['result'] = list(map(determineResult, df['nextClosePercentage']))
    df = addValue(df)

    # resultList = df['result'].values.tolist()
    # str_result = [str(i) for i in resultList]
    # print('Result count: ', Counter(str_result))
    
    # remove unused columns
    df.drop(['Adj Low', 'Adj High', 'Adj Open', 'Adj Volume', 'Ex-Dividend', 'Split Ratio', 'index', 'level_0'], axis=1, inplace=True, errors='ignore')
    #remove rows contains nan
    df = df.dropna(how='any')
    
    X = df[trainingDataColumns].values
    y = df['result'].values
    return X, y, df

def extract_featureForPredict(df, latestPredictionDate):
    df = df.replace('null', np.nan)
    # drop the rows if OHLCV is na
    df.dropna(how='any', subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)
    df = addValue(df)
    unPredicted_df = df.loc[(df["Date"] > latestPredictionDate)]
    unPredicted_df.reset_index(drop=True, inplace=True)
    X = unPredicted_df[trainingDataColumns].values
    return X, unPredicted_df

def getClassifier():
    return VotingClassifier([
            ('lsvc', svm.LinearSVC()),
            ('knn', neighbors.KNeighborsClassifier()),
            ('rfor', RandomForestClassifier())
            ])

def getClassifierAccuracy(symbol):
    quotes = getInitialData(symbol)
    if quotes is None or len(quotes) < MINIMUN_MACHINE_LEARNING_NUMBERS :
        return None, 0
    
    X, y, df = extract_featureset(quotes)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=TEST_SIZE)
    
    if not moreThanOneClassResult(y_train):
        print("only has 1 classes result, no training...")
        return None, 0
    
    clf = getClassifier()
    clf.fit(X_train, y_train)
    confidence=clf.score(X_test, y_test)
    return confidence, len(X_train)

def saveAccuracy(symbol):
    try:
        confidence, trainNumber = getClassifierAccuracy(symbol)
        if trainNumber > 0 : 
            stockMongo.saveLearnAccuracy(symbol, MODE, confidence, trainNumber)
    except Exception as e:
        print('...error while saveAccuracy for ', symbol)
        print(str(e))
        print('.............................')
    
def getPickleName(symbol):
    return symbol + "_" + MODE

def moreThanOneClassResult(array):
    uniqData = set(array)
    return len(uniqData) > 1

# pass in the symbol
# machine learning train the classifier
# pickle the classifier for future use
def initialMachineLearning(symbol):
    print(symbol)
    quotes = getInitialData(symbol)
    if quotes is None  or len(quotes) < MINIMUN_MACHINE_LEARNING_NUMBERS:
        return

    X, y, df = extract_featureset(quotes)
    
    if not moreThanOneClassResult(y):
        print("only has 1 classes result, no training...")
        return
    
    lastRecordDate = getLastRowDate(df)
    
    # clf = neighbors.KNeighborsClassifier()
    clf = getClassifier()
    clf.fit(X, y) # use full data for training
    data = {"lastRecordDate": lastRecordDate, "clf": clf}
    fileUtil.pickleIt(getPickleName(symbol), data)

# push new quote into machine learning    
def continueMachineLearning(symbol):
    ml_data = fileUtil.loadPickle(getPickleName(symbol))
    lastLearningDate = ml_data['lastRecordDate']
    quotes = stockMongo.findQuotesAfterDate(symbol, lastLearningDate)
    quotes = stockMongo.findLatestQuotes(symbol, 6 + len(list(quotes))) # need 7 quotes before the current one to get the 7 days mean 
    df = pd.DataFrame(list(quotes))
    X, y, df = extract_featureset(df)
    if (getFirstRowDate(df) > lastLearningDate):
        ml_data['clf'] = ml_data['clf'].fit(X, y)
        ml_data['lastRecordDate'] = getLastRowDate(df)
        fileUtil.pickleIt(getPickleName(symbol), ml_data)
    else:
        raise ValueError('try to learn data already learned...')
    
# get the latest quote from db and do the predict    
def predict(symbol):    
    ml_data = fileUtil.loadPickle(getPickleName(symbol))
    
    quotes = stockMongo.findLatestQuotes(symbol, 30)
    df = pd.DataFrame(list(quotes)[::-1])
    
    latestPrediction = list(stockMongo.findLatestPrediction(symbol, MODE))
    if latestPrediction is None or len(latestPrediction) == 0 : 
        latestPredictionDate = df['Date'].values[-2]
    else:
        latestPredictionDate = latestPrediction[0]['Date'] 
    X, df = extract_featureForPredict(df, latestPredictionDate)

    if X is None or df is None or len(df) == 0:
        print("no data to predict..." + symbol)
        return None

    date = getFirstRowDate(df)
    if(ml_data['lastRecordDate'] <= date): 
        prediction = ml_data['clf'].predict(X)
        print(symbol)
        print('X', X)
        print('df', df)
        print(prediction)
        return list(zip(prediction, df['Date'].values))
    else:
        raise ValueError('try to predict data already learned...')
    
def learn():
    symbols = pd.DataFrame(list(stockMongo.getAllActiveSymbols()))['Symbol'].values
#     symbols= ['BF.A']        
    with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
        p.map(initialMachineLearning, symbols)
        
    with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
        p.map(saveAccuracy, symbols)
        
def testOneClassifierAccuracy(): 
    print('confidence', getClassifierAccuracy('KO'))
    
def test20ClassifierAccuracy():
    symbols = ["KO","IBM","HPQ","DIS","CAT","ARNC","GE","DD","BA","UTX","MRO","MRK","JNJ","MMM","IP","XOM","MCD","GT","MO","HON"]
    for symbol in symbols:    
        print(symbol)
        print('confidence', getClassifierAccuracy('KO'))
    '''    
        initialMachineLearning(symbol)    
        data = fileUtil.loadPickle(getPickleName(symbol))
        print("last prediction", predict(symbol))
        # print('confidence', getClassifierAccuracy(symbol))
        print("--------------")
    '''
        
def others():
    '''
    initialMachineLearning('KO')    
    data1 = fileUtil.loadPickle(getPickleName('KO'))
    print(data1['lastRecordDate'])
    '''
    # print('confidence', getClassifierAccuracy('KO'))
    
    # print("last prediction", predict('KO'))
    
    # continueMachineLearning('KO')
    
if __name__ == '__main__':      
    multiprocessing.freeze_support() 
#     testOneClassifierAccuracy()
#     test20ClassifierAccuracy()
    learn()
        
