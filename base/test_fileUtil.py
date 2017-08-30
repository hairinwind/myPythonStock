from unittest import mock

from base import fileUtil


def test_getSymbolCsvFileName():
    assert fileUtil.getSymbolCsvFileName("XYZ", "2017-08-01", "2017-08-02") == "XYZ_2017-08-01_2017-08-02.csv"
    
    
def test_getSymbolFromFileName():
    assert fileUtil.getSymbolFromFileName("XYZ_2017-08-01_2017-08-02.csv") == "XYZ"

    
def test_symbolCannotBeCreatedAsFileOnWindows():
    assert fileUtil.symbolCannotBeCreatedAsFileOnWindows("CON")
    assert fileUtil.symbolCannotBeCreatedAsFileOnWindows("cON") 
    assert fileUtil.symbolCannotBeCreatedAsFileOnWindows("NUL")
    assert fileUtil.symbolCannotBeCreatedAsFileOnWindows("PRN")       
    assert not fileUtil.symbolCannotBeCreatedAsFileOnWindows("KO")  

    
@mock.patch('pandas.DataFrame')    
def test_saveQuotesToCsv(mocked_quotes):
    start = "2017-08-01"
    end = "2017-08-02"
    symbol = "TEST"
    fileUtil.saveQuotesToCsv(symbol, mocked_quotes, start, end)
    assert mocked_quotes.to_csv.called
    mocked_quotes.to_csv.assert_called_once_with(fileUtil.QUOTES_DIR + fileUtil.getSymbolCsvFileName(symbol, start, end))
    
    mocked_quotes.reset_mock()
    fileUtil.saveQuotesToCsv(symbol, None, start, end)
    assert not mocked_quotes.to_csv.called

    
@mock.patch('base.fileUtil.archiveToZipFile')    
def test_archiveQuoteFileToZip(mocked_archiveToZipFile):
    targetDir = "targetDir"
    fileFullPath = "/x/y/z/abc.csv"
    fileUtil.archiveQuoteFileToZip(targetDir, fileFullPath)
    mocked_archiveToZipFile.assert_called_once_with(targetDir, fileFullPath, "abc.zip")


@mock.patch('os.remove')
@mock.patch('zipfile.ZipFile')
def test_archiveToZipFile(mocked_zipfile, mocked_osRemove):
    targetDir = "targetDir/"
    fileFullPath = "/x/y/z/abc.csv"
    zipFileName = "abc.zip" 
    mocked_zipfile.return_value = mocked_zipfile
    fileUtil.archiveToZipFile(targetDir, fileFullPath, zipFileName)
    mocked_zipfile.assert_called_once_with(targetDir + zipFileName, mode='a')
    mocked_osRemove.assert_called_once_with(fileFullPath)
    mocked_zipfile.close.assert_called_once()
    
    
