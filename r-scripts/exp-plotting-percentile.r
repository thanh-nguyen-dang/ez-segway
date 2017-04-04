#!/usr/bin/env Rscript

require(ggplot2)
require(data.table)
library(reshape2)
require(plyr)
library(grid)
# require(Hmisc)
library(plyr)

source("summarySE.r")

file <- "update_time.log"

args <- commandArgs(trailingOnly = TRUE)
if(length(args) > 0)
{
	file <- args[1]
}

alldata <- as.data.frame(read.table(file, header=TRUE, sep="\t"))

asfactors <- c("arg_method","arg_topology")
for (i in asfactors) {
    alldata[[i]] <- as.factor(alldata[[i]])
}
alldata$arg_method <- revalue(alldata$arg_method, c("cen"="Cent.", "ez"="ez-Seg."))
alldata$arg_topology <- revalue(alldata$arg_topology, c("b4"="B4", "i2"="I2"))

data <- alldata


Xquantified50 <- ddply(alldata,c("arg_method","arg_topology"),summarise,time = quantile(update_only, probs = 0.50),type="Coordination", perc=50)

Xquantified90 <- ddply(alldata,c("arg_method","arg_topology"),summarise,time = quantile(update_only, probs = 0.90),type="Coordination", perc=90)

Xquantified99 <- ddply(alldata,c("arg_method","arg_topology"),summarise,time = quantile(update_only, probs = 0.99),type="Coordination", perc=99)

CTRquantified50 <- ddply(alldata,c("arg_method","arg_topology"),summarise,time = quantile(ctr_time, probs = 0.5),type="Computation", perc=50)

CTRquantified90 <- ddply(alldata,c("arg_method","arg_topology"),summarise,time = quantile(ctr_time, probs = 0.90),type="Computation", perc=90)

CTRquantified99 <- ddply(alldata,c("arg_method","arg_topology"),summarise,time = quantile(ctr_time, probs = 0.99),type="Computation", perc=99)

quantified <- rbind(CTRquantified50, CTRquantified90, CTRquantified99,Xquantified50, Xquantified90, Xquantified99)

quantified <- ddply(quantified,c("arg_method"),transform,group=paste(arg_topology," ",perc,"th-ile",sep=""))

p1 <- ggplot() +
  geom_bar(data=quantified, aes(y = time, x = arg_method, fill = type), stat="identity",
           position='stack',colour="black",
             size=.3) +
  facet_grid( ~ group) +
  labs(y="Time [ms]",fill = "Update time") +
    theme_bw() +
  guides(fill = guide_legend(override.aes = list(colour = NULL))) +
  theme(plot.margin=unit(x=c(1,1,1,1),units="mm"),
        axis.title.x=element_blank(),
        axis.text.x = element_text(size=8),
        legend.title = element_blank(), legend.key = element_rect(colour = "black"), legend.position="top", legend.margin=unit(-0.225, 'cm')) +
  scale_y_continuous(breaks = c(0,200,400,600,800,1000,1200,1400,1600,1800,2000,2200,2400))

ggsave("plot-percentile.pdf", width=6.5, height=2.5)
