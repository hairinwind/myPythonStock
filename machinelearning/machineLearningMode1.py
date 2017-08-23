from base import stockMongo
from quote import stockData
import pandas as pd
from base import fileUtil
from collections import Counter
from sklearn import svm, neighbors
from sklearn.model_selection import train_test_split
from sklearn.ensemble import VotingClassifier, RandomForestClassifier

MODE = 'mode1'
trainingDataColumns = ['Open', 'High', 'Low', 'Close', 'Volume', '3_mean', '7_mean', '3_mean_volume', '7_mean_volume', 'closeOpenPercentage']
MINIMUN_MACHINE_LEARNING_NUMBERS = 500
TEST_SIZE = 0.25

def determineResult(value):
    threshold1 = 0.02
    threshold2 = -0.02
    if value > threshold1:
        return 1
    if value < threshold2:
        return -1
    return 0  

def getQuoteData(symbol):
    quotes = stockMongo.findAllQuotesBySymbol(symbol)
    df = pd.DataFrame(list(quotes))
    #df.set_index('Date', inplace=True)
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
    quotes = getQuoteData(symbol)
    if len(quotes) < MINIMUN_MACHINE_LEARNING_NUMBERS :
        return None, 0
    
    X, y, df = extract_featureset(quotes)
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=TEST_SIZE)
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

# pass in the symbol
# machine learning train the classifier
# pickle the classifier for future use
def initialMachineLearning(symbol):
    print(symbol)
    X, y, df = extract_featureset(getQuoteData(symbol))
    
    if len(df) < MINIMUN_MACHINE_LEARNING_NUMBERS or X is None or y is None:
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
    if ml_data is None: 
        return
    
    quotes = stockMongo.findLatestQuotes(symbol, 30)
    df = pd.DataFrame(list(quotes)[::-1])
    
    latestPrediction = list(stockMongo.findLatestPrediction(symbol, MODE))
    if latestPrediction is None or len(latestPrediction) == 0: 
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

if __name__ == '__main__':       
    '''
    initialMachineLearning('KO')    
    data1 = fileUtil.loadPickle(getPickleName('KO'))
    print(data1['lastRecordDate'])
    '''
    # print('confidence', getClassifierAccuracy('KO'))
    
    # print("last prediction", predict('KO'))
    
    # continueMachineLearning('KO')
    
    
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
    
    # symbols = pd.DataFrame(list(stockMongo.getAllSymbols()))['Symbol'].values
    '''symbols = ["ANDAR"]
    import multiprocessing
    multiprocessing.freeze_support()        
    with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
        # p.map(saveAccuracy, symbols)
        p.map(initialMachineLearning, symbols)'''
        
        
