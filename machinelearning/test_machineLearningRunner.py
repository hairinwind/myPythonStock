from unittest import mock
from machinelearning import machineLearningRunner
import pandas as pd


def test_removeUnusedColumnsAndRows():
    df = pd.DataFrame([{'Adj Low':0, 'Adj High':1, 'Adj Open':2, 'Adj Volume':3, 'Ex-Dividend':4, 'Split Ratio':5, 'index':6, 'level_0':7, 'otherField':8, 'Open':9, 'High':10, 'Low':11, 'Close':12, 'Volume':13},
                       {'Adj Low':0, 'Adj High':1, 'Adj Open':2, 'Adj Volume':3, 'Ex-Dividend':4, 'Split Ratio':5, 'index':6, 'level_0':7, 'otherField':8, 'Open':9, 'High':10, 'Low':11, 'Close':12}])
    df = machineLearningRunner.removeUnusedColumnsAndRows(df)
    assert len(df) == 1
    assert not {'Adj Low', 'Adj High', 'Adj Open', 'Adj Volume', 'Ex-Dividend', 'Split Ratio', 'index', 'level_0'}.issubset(df.columns)
    
    
@mock.patch('machinelearning.machineLearningMode2.machineLearningMode2')    
def test_getQuoteData(mocked_machineLearningMode):
    startDate = '2017-06-15'
    endDate = '2017-06-17'  # not included
    symbol = 'KO'
    mocked_machineLearningMode.QUOTES_NUMBER = 100
    df = machineLearningRunner.getQuoteData(mocked_machineLearningMode, startDate, endDate, symbol)
    assert len(df) == 2
    
    df = machineLearningRunner.getQuoteData(mocked_machineLearningMode, startDate, endDate, symbol, history_days=3)
    assert len(df) == 5
    assert not {'Adj Low', 'Adj High', 'Adj Open', 'Adj Volume', 'Ex-Dividend', 'Split Ratio', 'index', 'level_0'}.issubset(df.columns)
    

def test_moreThanOneClassResult():
    assert machineLearningRunner.moreThanOneClassResult([1, 1, 0]) == True
    assert machineLearningRunner.moreThanOneClassResult([1, 1, 1]) == False
    assert machineLearningRunner.moreThanOneClassResult(None) == False
    

def test_getLastRowDate():
    x = pd.DataFrame({'Date': [1, 2, 3], 'y': [3, 4, 5]})
    '''
       Date y
    0  1   3
    1  2   4
    2  3   5
    '''
    assert machineLearningRunner.getLastRowDate(x) == 3
    
    
def test_getPickleName():
    assert machineLearningRunner.getPickleName('symbolX', 'modeY') == 'symbolX_modeY' 
    

@mock.patch('base.fileUtil.pickleIt')
@mock.patch('sklearn.ensemble.VotingClassifier')
@mock.patch('machinelearning.machineLearningMode2.machineLearningMode2.getClassifier')
@mock.patch('machinelearning.machineLearningMode2.machineLearningMode2.extract_featureset')    
@mock.patch('machinelearning.machineLearningMode2.machineLearningMode2')    
def test_initialMachineLearning(mocked_machineLearningMode, mocked_extract_featureset, mocked_getClassifier, mocked_classifier, mocked_pickleIt):
    startDate = '2017-06-15'
    endDate = '2017-06-17'  # not included
    symbol = 'KO'
    mocked_machineLearningMode.MODE = 'modeTest'
    mocked_machineLearningMode.QUOTES_NUMBER = 100
    mocked_machineLearningMode.MINIMUN_MACHINE_LEARNING_NUMBERS = 100
    result = machineLearningRunner.initialMachineLearning(mocked_machineLearningMode, startDate, endDate, symbol)
    assert result == None
    
    mocked_machineLearningMode.MINIMUN_MACHINE_LEARNING_NUMBERS = 1
    mocked_machineLearningMode.extract_featureset = mocked_extract_featureset
    X = [[1], [2], [3]]
    y = [1, 1, 0]
    df = pd.DataFrame({'Date': [1, 2, 3], 'y': [3, 4, 5]})
    mocked_extract_featureset.return_value = (X, y, df)
    mocked_machineLearningMode.getClassifier = mocked_getClassifier
    mocked_getClassifier.return_value = mocked_classifier
    result = machineLearningRunner.initialMachineLearning(mocked_machineLearningMode, startDate, endDate, symbol)
    mocked_extract_featureset.assert_called_once()
    # classifier.fit.assert_called_once()
    mocked_pickleIt.assert_called_once_with("KO_modeTest", {"lastRecordDate": 3, "clf": mocked_classifier})
    
    
    
    
    
