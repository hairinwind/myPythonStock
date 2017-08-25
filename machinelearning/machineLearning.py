from machinelearning.machineLearningMode1 import machineLearningMode1
from machinelearning.machineLearningMode2 import machineLearningMode2

machineLearningModes = [machineLearningMode1(), machineLearningMode2()]
MIN_PREDICTION_COUNT = 4000

def checkPredictionCount(df, machineLearningMode, text="prediction"):
    if len(df) == 0 :
        return 0
    countResult = df.loc[df['_id'] == machineLearningMode.MODE]
    if len(countResult) > 0 :   
        if countResult['count'].values[0] <= MIN_PREDICTION_COUNT :
            print('{} for {} is less than {}'.format(text, machineLearningMode.MODE, MIN_PREDICTION_COUNT)) 
    else:
        print('{} for {} not found'.format(text, machineLearningMode.MODE))
    return countResult
