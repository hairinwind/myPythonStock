

def determineResult(threshold1, threshold2, value):
    if value > threshold1:
        return 1
    if value < threshold2:
        return -1
    return 0  