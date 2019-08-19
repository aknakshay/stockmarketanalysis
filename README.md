# Stock Market Analysis
Using the power of Big Data Tools to analyze Stock Market

Stocks selected: 

  **NASDAQ**: GOOGL,MSFT,ORCL,FB,AAPL,TSLA
  
  **NSE**: TCS,INFY

## Data Retrieval
###       Stock Prices

For collection of per minute Stock prices, Alphavantage API is used to retrieve prices for companies listed on NASDAQ. Read in Detail [here](https://www.cloudsigma.com/twitter-data-analysis-using-python/)

For NSE, a scraper is written in Python which scrapes the latest prices for each minute. Read in Detail [here](https://bullseyestock.wordpress.com/2018/01/21/pulling-nse-per-minute-data-using-python/).

However the problem faced here was that at certain 1 or 2 minute interval, price won't get updated on the NSE website. For the same, data interpolation is done. 

###       Twitter Data

Collected Twitter Data using Python with Twitter API. Read in Detail [here](https://www.cloudsigma.com/twitter-data-analysis-using-python/).

Also collected Twitter Data using Flume. Had to modify Flume's Twitter's package code for the same. Read in Detail [here](https://www.cloudsigma.com/realtime-twitter-data-ingestion-using-flume/)

## Data Collection
Data from Twitter is stored on the Data Lake. For the purpose of this project, Cloudera Datalake has been used.

## Data Preparation
The twitter data is processed to correct the spellings of the text. It is done using a JAVA library called Language Tool.

## Model - Sentiment Analysis
On the twitter data, Stanford Core NLP library is used to tokenize, annotate sentences, part of speech tagging, syntactic analysis and sentiment analysis using Stanford's pre-trained model. With the same, sentiment value of each tweet is obtained. 
Multiplication of Number of followers and Sentiment value for that tweet is aggregated per minute
Read in detail [here](https://www.cloudsigma.com/sentiment-analysis-of-twitter-using-spark/)

Read about our progress on [our blog](http://bullseyestock.wordpress.com) 
