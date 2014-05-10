# plotter.R

mydata = read.csv("load.csv")

responsetime = mydata$responsetime

mymean = mean(responsetime)

par(xlog=FALSE)
Fn = ecdf(responsetime)

summary(responsetime)

#plot(Fn, main="Response times", xlab="response time (ms)", ylab="cumulative response proportion", xlim=c(1,500), log="x")
plot(Fn, main="Response times", xlab="response time (ms)", ylab="cumulative response proportion", xlim=c(1,500), log="x")

library(Hmisc)
g <- c( rep('Response Time (ms)',length(responsetime)) )

Ecdf(responsetime, group=g, 
	main="Response time distribution", 
	xlab='Response Time ms ',
	ylab = "percentile of requests",
	datadensity="none", 
	#log="x",
	q=c(.50,.90,.99,.999),
	xlim=range(0,5)
	)
