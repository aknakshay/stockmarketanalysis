import urllib.request
import json
import os

def import_web(ticker):
    """
    :param identifier: List, Takes the company name
    :return:displays companies records per minute
    """
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol='+ticker +'&interval=1min&apikey=' + YOUR_API_KEY + '&outputsize=full&datatype=json'
    fp = urllib.request.urlopen(url)
    mybytes = fp.read()
    mystr = mybytes.decode("utf8")
    fp.close()
    return mystr


def get_value(ticker):
    js = import_web(ticker)
    parsed_data = json.loads(js) # loads the json and converts the json string into dictionary
    ps = parsed_data['Time Series (1min)']
    partitionSave(ps,ticker)

            
def partitionSave(ps,ticker):
    date = {}
    for i in ps:
        date[i[:10]] = "date"
    for d in date.keys():
        tmp = {}
        for i in ps:
            if(i[:10] == d):
                tmp[i] = ps[i]
        if(os.path.isdir(d) == False):
            os.mkdir(d)
        fname = ticker + "_dann"
        try:
            with open(os.path.join(d,fname),'r') as f:
                t = json.load(f)
                for i in t:
                    tmp[i]=t[i]
        except Exception as e:
            pass
                
        with open(os.path.join(d,fname), 'w') as f:
            json.dump(tmp, f)
                
def main():
    #Start Process
    company_list = ['GOOGL','MSFT','ORCL','FB','AAPL','TSLA'];
    try:
        for company in company_list:
            print("Starting with " + company)
            get_value(company)
            print("Ended Writing Data of " + company)
    except Exception as e:
        print(e)

main()
