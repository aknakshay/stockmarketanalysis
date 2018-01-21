import oauth2
import json
import time
import datetime
import os
import sys

#Usage
#python script.py <Start-Date[YYYY-MM-DD]> <End-Date[YYYY-MM-DD]> keyword

start_date = sys.argv[1] #"2018-01-09" 
end_date = sys.argv[2] #"2018-01-10" 
consumerKey="Enter_Your_Consumer_Key_Here"
consumerSecret="Enter_Your_Consumer_Secret_Here"
accessToken="Enter_Your_Access_Token_Here"
accessTokenSecret="Enter_Your_Access_Token_Secret_Here"
keyword= sys.argv[3] #"tcs"
lang="en" #see what twitter offers for language filtering

data = {}

req_count = 0




def oauth_req(url, http_method="GET", post_body=b"", http_headers=None):
    global req_count,consumerKey,consumerSecret,accessToken,accessTokenSecret
    req_count += 1
    
    consumer = oauth2.Consumer(key=consumerKey, secret=consumerSecret)
    token = oauth2.Token(key=accessToken, secret=accessTokenSecret)
    client = oauth2.Client(consumer, token)
    resp, content = client.request( url, method=http_method, body=post_body , headers=http_headers )
    return content


def get_tweets(min_faves):
    global keyword, start_date, end_date, lang
    return oauth_req( 'https://api.twitter.com/1.1/search/tweets.json?' + '&q=' + keyword + '&lang=' + lang + '%20since%3A' + start_date + '%20until%3A' + end_date + '%20min_faves%3A' + str(min_faves) +'&result_type=mixed&count=100')

min_faves=60000
change=10000 #high reduction in min_faves to extract data
interval = 500 #normal reduction in min_faves to extract data

t_last = time.time()

def autosave(saveOverride = False):
    global t_last
    saveStatus = (time.time() > t_last + 300)
    if(saveOverride == True):
        saveStatus = True
        
    if(saveStatus):
        t_last=time.time()
        tmp = {}
        print("Autosave at " + str(datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")))
        fnamea = keyword + "-st-" + start_date + "-ed-"+ end_date + '.json'
        if os.path.exists(fnamea) == True:
            with open(fnamea,'r+') as f:
                tmp = json.load(f)
        for i in data.keys():
            tmp[i] = data[i]
        with open(fnamea,'w+') as f:
            json.dump(tmp,f)
            
            
while(1): 
    d = json.loads(get_tweets(min_faves))
    try:
        for i in d['statuses']:
            data[i['id']] = i
        c = len(d['statuses'])
    except Exception as e:
        print("Error at request : " + str(req_count))
        autosave(True)
      
    print("At request: " + str(req_count) + "  Total Tweets Collected: " + str(len(data)) + " with Min Faves: " + str(min_faves) )
    if c==100 and min_faves>10000:
        if (change>1000):
            change /= 2
            min_faves += change
        else:
            min_faves -= change
        
    elif min_faves>10000:
        min_faves -= change
    
    else:
        min_faves -= interval
        if(min_faves < 0):
            fnamea = keyword + '.json'
            autosave(True)
            break
    autosave()
    time.sleep(5)
    
print("End")
