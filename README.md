# Stock Market Analysis
Using the power of Big Data Tools to analyze Stock Market

Stocks selected: 

  NASDAQ: GOOGL,MSFT,ORCL,FB,AAPL,TSLA
  
  NSE: TCS,INFY

## Data Retrieval
###       Stock Prices

For collection of per minute Stock prices, Alphavantage API is used to retrieve prices for companies listed on NASDAQ. Read in Detail [here](https://www.cloudsigma.com/twitter-data-analysis-using-python/)

For NSE, a scraper is written in Python which scrapes the latest prices for each minute. Read in Detail [here](https://bullseyestock.wordpress.com/2018/01/21/pulling-nse-per-minute-data-using-python/).

However the problem faced here was that at certain 1 or 2 minute interval, price won't get updated on the NSE website. For the same, data interpolation is done. 

###       Twitter Data

https://www.cloudsigma.com/realtime-twitter-data-ingestion-using-flume/

Read about our progress on [our blog](http://bullseyestock.wordpress.com) 
