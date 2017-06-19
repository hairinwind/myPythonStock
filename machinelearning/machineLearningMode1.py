from base import stockMongo
import pandas as pd
from base import fileUtil
from collections import Counter
from sklearn import svm, cross_validation, neighbors
from sklearn.ensemble import VotingClassifier, RandomForestClassifier

trainingDataColumns = ['Open', 'High', 'Low', 'Close', 'Volume', '3_mean', '7_mean', '3_mean_volume', '7_mean_volume', 'closeOpenPercentage']

def determineResult(value):
    threshold1 = 0.015
    threshold2 = -0.015
    if value > threshold1:
        return 1
    if value < threshold2:
        return -1
    return 0  

def getInitialData(symbol):
    quotes = stockMongo.findAllQuotesBySymbol(symbol)
    df = pd.DataFrame(list(quotes))
    #df.set_index('Date', inplace=True)
    return df

#extract the dataframe to the featuerset for machine learning

def addValue(df):
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

def extract_featureset(df):
    df = df.sort_values(['Date'])
    df['nextClose'] = df['Close'].shift(-1)
    df['nextClosePercentage'] = (df['nextClose']-df['Close'])/df['Close']
    df['result'] = list(map(determineResult, df['nextClosePercentage']))
    
    df = addValue(df)

    # resultList = df['result'].values.tolist()
    # str_result = [str(i) for i in resultList]
    # print('Result count: ', Counter(str_result))
    
    #remove rows contains nan
    df = df.dropna(how='any')
    
    X = df[trainingDataColumns].values
    y = df['result'].values
    return X, y, df

def extract_featureForPredict(df):
    df = addValue(df)
    df = df.dropna(how='any')
    X = df[trainingDataColumns].values
    return X, df

def getClassifier():
    return VotingClassifier([
            ('lsvc', svm.LinearSVC()),
            ('knn', neighbors.KNeighborsClassifier()),
            ('rfor', RandomForestClassifier())
            ])

def getClassifierAccuracy(symbol):
    X, y, df = extract_featureset(getInitialData(symbol))
    X_train, X_test, y_train, y_test = cross_validation.train_test_split(X, y, test_size=0.1)
    clf = getClassifier()
    clf.fit(X_train, y_train)
    confidence=clf.score(X_test, y_test)
    return confidence, len(X_train)

def saveAccuracy(symbol):
    try:
        confidence, trainNumber = getClassifierAccuracy(symbol)
        stockMongo.saveLearnAccuracy(symbol, 'mode1', confidence, trainNumber)
    except Exception as e:
        print('...error while saveAccuracy for ', symbol)
        print(str(e))
        print('.............................')
    
def getPickleName(symbol):
    return symbol + "_mode1"

# pass in the symbol
# machine learning train the classifier
# pickle the classifier for future use
def initialMachineLearning(symbol):
    X, y, df = extract_featureset(getInitialData(symbol))
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
    quotes = stockMongo.findLatestQuotes(symbol, 7)
    df = pd.DataFrame(list(quotes)[::-1])
    X, df = extract_featureForPredict(df)
    date = getLastRowDate(df)
    
    ml_data = fileUtil.loadPickle(getPickleName(symbol))
    if(ml_data['lastRecordDate'] <= date): 
        prediction = ml_data['clf'].predict(X)
        return prediction
    else:
        raise ValueError('try to predict data already learned...')

if __name__ == '__main__':       
    '''
    initialMachineLearning('KO')    
    data1 = fileUtil.loadPickle(getPickleName('KO'))
    print(data1['lastRecordDate'])
    '''
    # print('confidence', getClassifierAccuracy('KO'))
    
    # print("last prediction", predict('KO'))
    
    # continueMachineLearning('KO')
    
    '''
    symbols = ["KO","IBM","HPQ","DIS","CAT","ARNC","GE","DD","BA","UTX","MRO","MRK","JNJ","MMM","IP","XOM","MCD","GT","MO","HON"]
    for symbol in symbols:    
        print(symbol)
        initialMachineLearning(symbol)    
        data = fileUtil.loadPickle(getPickleName(symbol))
        print("last prediction", predict(symbol))
        print("--------------")
    '''
    
    symbols = pd.DataFrame(list(stockMongo.getAllSymbols()))['Symbol'].values
    import multiprocessing
    multiprocessing.freeze_support()        
    with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
        p.map(saveAccuracy, symbols)