from base import dateUtil
import datetime as dt

def test_toDate():
    dateResult = dateUtil.toDate("2017-08-01")
    assert dateResult.year == 2017
    assert dateResult.month == 8
    assert dateResult.day == 1
    

def test_toString():
    date = dt.datetime(2017, 8, 1)
    dateString = dateUtil.toString(date)
    assert dateString == "2017-08-01"
    

def test_addDays():
    dateResult = dateUtil.addDays("2017-08-01", 3)
    dateStringResult = dateUtil.toString(dateResult)
    assert dateStringResult == '2017-08-04'
    
    dateResult = dateUtil.addDays("2017-08-01", -1)
    dateStringResult = dateUtil.toString(dateResult)
    assert dateStringResult == '2017-07-31'
    
