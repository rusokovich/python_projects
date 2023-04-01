import urllib.request
outfilename = "apples.xls"
url_of_file = "https://www.ers.usda.gov/webdocs/DataFiles/51035/apples.xlsx"
urllib.request.urlretrieve(url_of_file, outfilename)
print("I have downloaded the file")
