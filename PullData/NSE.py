import urllib.request
import json
import time

data_infy = {}
data_tcs = {}
lsave=time.time()

def autoSave():
	global lsave
	curr_time = time.time()
	if(curr_time >= lsave + 300):
		with open('infy','a+') as f:
			f.write(str(data_infy))
		with open('tcs','a+') as f:
			f.write(str(data_tcs))
		lsave = time.time()
		combiner()
		print("AutoSaved at : "+ time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(lsave)))

def import_web(ticker):
    """
    :param ticker: Takes the company ticker
    :return: Returns the HTML of the page
    """
    url = 'https://www.nseindia.com/live_market/dynaContent/live_watch/get_quote/GetQuote.jsp?symbol='+ticker+'&illiquid=0&smeFlag=0&itpFlag=0'
    req = urllib.request.Request(url, headers={'User-Agent' : "Chrome Browser"}) 
    fp = urllib.request.urlopen(req, timeout=10)
    mybytes = fp.read()
    mystr = mybytes.decode("utf8")
    fp.close()
    return mystr


def get_quote(ticker):
    """
    :param ticker: Takes the company ticker
    :return: None
    """
    ticker = ticker.upper()
    try:
        """fetches a UTF-8-encoded web page, and  extract some text from the HTML"""
        string_html = import_web(ticker)
        get_data(filter_data(string_html),ticker)
    except Exception as e:
        print(e)

          
def filter_data(string_html):          
    searchString = '<div id="responseDiv" style="display:none">'
    #assign: stores html tag to find where data starts
    searchString2 = '</div>'
    #stores:  stores html tag where  data end
    sta = string_html.find(searchString)
    # returns & store: find() method returns the lowest index of the substring (if found). If not found, it returns -1.
    data = string_html[sta + 43:]
    #returns & stores: skips 43 characters and stores the index of substring
    end = data.find(searchString2)
    # returns & store: find() method returns the lowest index of the substring (if found). If not found, it returns -1.
    fdata = data[:end]
    #fetch: stores the fetched data into fdata
    stripped = fdata.strip()
    #removes: blank spaces
    return stripped



def get_data(stripped, company):
    js = json.loads(stripped)
    datajs = js['data'][0]
    subdictionary = {}
    subdictionary['1. open'] = datajs['open']
    subdictionary['2. high'] = datajs['dayHigh']
    subdictionary['3. low'] = datajs['dayLow']
    subdictionary['4. close'] = datajs['lastPrice']
    subdictionary['5. volume'] = datajs['totalTradedVolume']
    if company == 'INFY':
        print (
            'Adding value at : ',
            js['lastUpdateTime'],
            ' to ',
            company,
            ' Price:',
            datajs["lastPrice"],
            )
        data_infy[js['lastUpdateTime']] = subdictionary
    elif company == 'TCS':
        print (
            'Adding value at : ',
            js['lastUpdateTime'],
            ' to ',
            company,
            ' Price:',
            datajs["lastPrice"],
            )
        data_tcs[js['lastUpdateTime']] = subdictionary

         
def combiner():
	file_names = ['infy','tcs']

	for ticker in file_names:
		final = {}

		with open(ticker,'r') as f:
			data = f.read()
		data = data.replace("}{","}split{")
		splittedData = data.split('split')
		
		for dictionary in splittedData:
			tmp = json.loads(dictionary.replace("'",'"'))
			for key in tmp.keys():
				final[key] = tmp[key]
	
		newFileName = ticker
		with open(newFileName,'w') as fw:
			fw.write(str(final))	
	

def main():
	t_list=['TCS','INFY']
	try:
		while(True):
			for ticker in t_list:
				print("Starting get_quote for ",ticker)
				get_quote(ticker)
			autoSave()
			print("Taking a nap! Good Night")
			time.sleep(30)
			print("\n\n")
	except Exception as e:
		print(e)
	finally:
		with open('infy','a+') as f:
			f.write(str(data_infy))
		with open('tcs','a+') as f:
			f.write(str(data_tcs))
		combiner()
        
main()
