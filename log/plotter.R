# plotter.R

mydata = read.csv("load.csv")

responsetime = mydata$responsetime

mymean = mean(responsetime)

Fn = ecdf(responsetime)
par(xlog=TRUE)

#plot(Fn, main="Response times", xlab="response time (ms)", ylab="cumulative response proportion", xlim=c(1,500), log="x")
plot(Fn, main="Response times", xlab="response time (ms)", ylab="cumulative response proportion", xlim=c(1,500), log="x")

library(Hmisc)
g <- c( rep('Response Time (ms)',length(responsetime) ) )

Ecdf(responsetime, main="Response time distribution", xlab='Response Time ms (log scale)',
	ylab = "percentile of requests", 
	q=c(.50,.90,.999), log="x")
