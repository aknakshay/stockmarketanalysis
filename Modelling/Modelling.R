Loading a data sample into R

```{r}
getwd()
setwd ("./NSE/output")

infy_t <- read.csv('infy.csv',sep = '|',stringsAsFactors = FALSE)

remCom <- function(data){
  as.numeric(gsub(",", "", data))
}
 


infy <- data.frame(strptime(paste(substr(infy_t[,1],1,18),"00",sep=""),format = "%d-%b-%Y %T"), sapply(infy_t[2:6],remCom))

names(infy) <- c("TimeStamp","Open","High","Low","Close","Volume")
infy <- infy[ order(infy$TimeStamp),]
head(infy)
```


```{r}
getwd()
setwd ("./NASDAQ/output")
apple_t <- read.csv('AAPL_dann.csv',sep = '|')

apple <- data.frame(strptime((apple_t[,1]),format = "%Y-%m-%d %T",tz='EST'),apple_t[,2:ncol(apple_t)])

names(apple) <- c("TimeStamp","Open","High","Low","Close","Volume")

apple <- apple[ order(apple$TimeStamp),]
head(apple)
```




Finding missing data points in a given data frame #infosys
NSE timings are from 9.30 AM to 4 PM
```{r}

start <- strptime("2017-12-19 09:00:00",format = "%Y-%m-%d %T")
end <- strptime("2017-12-19 16:00:00",format = "%Y-%m-%d %T")

#start <- as.Date("2017-12-15 09:00:00",format = "%d-%b-%Y %T")
#end   <- as.Date("2018-01-20 16:00:00",format = "%d-%b-%Y %T")

theDate <- start
count = 0
total = 0
while (theDate <= end){
  total = total + 1
  if(sum(theDate==infy$TimeStamp)!=1) 
  {#print(theDate);
    count=count+1}
  
  theDate <- theDate + 60      
}
print(c(format(theDate,format="%Y-%m-%d"),total,count))
```




Finding missing data points in a given data frame #apple
NASDAQ timings are from 9.30 AM to 4 PM
```{r}

start <- strptime("2017-12-07 09:30:00",format = "%Y-%m-%d %T",tz='EST')
end <- strptime("2017-12-07 16:00:00",format = "%Y-%m-%d %T",tz='EST')


theDate <- start
count = 0
total = 0
while (theDate <= end){
  total = total + 1
  #if(!(theDate %in% apple$TimeStamp)) {print(theDate);count=count+1}
  if(sum(theDate==apple$TimeStamp)!=1) 
  {#print(theDate);
    count=count+1}
  
  theDate <- theDate + 60                    
}
print(c(format(theDate,format="%Y-%m-%d"),total,count))


```





Concave Function : Replaces NA with concave values

```{r}
test[1:11] <- NA
concave(test)



```

```{r}

concave <- function(data.set){
naloc <- which(is.na(data.set)) #5,6,7

for (val in naloc){
  #print(c("Starting with",val)) #5

  #Finding y
  
  y=NA                 #next value in x....y
  tmp=val+1                 #checking if there's any value on 6
  
  while(is.na(y)){             #till it doesn't get the next optimal value
  if(!((tmp) %in% which(is.na(data.set))))    #check if 6 has value
{     
    #print(c("y=",tmp))             #if it has,print 6
    y = tmp                 #and set y to tmp to break the loop
  } else{
    tmp = tmp+1             #else check on 7
  }
  }
  
  #Finding x
    
  x=NA                 #previous value in x....y
  tmp=val-1                 #checking if there's any value on 4
  
  while(is.na(x)){             #till it doesn't get the next optimal value
  if(!((tmp) %in% which(is.na(data.set))))    #check if 4 has value
{ 
    #print(c("x=",tmp))             #if it has,print 6
    x = tmp                 #and set y to tmp to break the loop
  } else{
    tmp = tmp-1             #else check on 7
  }
  }
 
    if(x==0){
      data.set[val]=data.set[y]
    }
    else if( sum(is.na(c(data.set[x],data.set[y]))) == 2 ){
      data.set[val] = NA
      #print(c("NA value at ",val))
    }
    else if(sum(is.na(data.set[x]))==1){
     data.set[val] = data.set[y] 
    }
    else if(sum(c(is.na(data.set[y])))==1){
      data.set[val] = data.set[x]
    } else{
      data.set[val] = (data.set[x]+data.set[y])/2
    }
    
}

return(data.set)
}

```

String manipulations for date loop
```{r}

sfunc <- function(str){
  substr(str,1,10)
}



  #tmp <- infy
tmp <- data.frame(infy[FALSE,])

for(da in levels(as.factor(sapply(infy[1], sfunc)))){
  # starting for date "2017-12-15"
  start <- strptime(paste(da,"09:00:00"),format = "%Y-%m-%d %T")
  end <- strptime(paste(da,"16:00:00"),format = "%Y-%m-%d %T")
  #start <- strptime(paste("2017-12-15","09:00:00"),format = "%Y-%m-%d %T")
  #end <- strptime(paste("2017-12-15","16:00:00"),format = "%Y-%m-%d %T")

  theDate <- start

  daydata <- data.frame(infy[FALSE,])
  #names(daydata) <- names(infy)
  
  count = 0
  total = 0
  while (theDate <= end){
    total = total + 1
    if(sum(theDate==infy$TimeStamp)!=1) 
        {#print(theDate);
        count=count+1
        t1 <- data.frame(theDate,NA,NA,NA,NA,NA)
        names(t1) <- names(daydata)
        daydata <- rbind(daydata,t1)
    }
    else{
      #daydata <- rbind(daydata,infy[1,])
      daydata <- rbind(daydata,infy[which(theDate==infy$TimeStamp),])
    }
  
  theDate <- theDate + 60   
  }
  
  as.numeric(NA)

   # daydata <- data.frame(daydata[1],sapply(daydata[2:6],as.numeric))
  
  daydata[2:6] <- data.frame(sapply(daydata[2:6],concave))
#concave(daydata[,2])
  tmp <- rbind(tmp,daydata)
}

tmp <- tmp[ order(tmp$TimeStamp),]


```


```{r}
plot(tmp$TimeStamp,tmp$Close,type='l')
```

