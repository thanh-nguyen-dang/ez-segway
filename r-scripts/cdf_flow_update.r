#!/usr/bin/env Rscript

require(ggplot2)
# require(data.table)
library(reshape2)
require(plyr)
library(grid)
# require(Hmisc)

source("summarySE.r")

# CDF drawing

loadone <- function(file) {
    args <- commandArgs(trailingOnly = TRUE)
    if(length(args) > 0)
    {
        file <- args[1] 
    }

    data <- as.data.frame(read.table(file, header=TRUE, sep="\t"))
    asfactors <- c("arg_method")
    for (i in asfactors) {
        data[[i]] <- as.factor(data[[i]])
    }
    return(data)
}


loadall <- function(basedir, exps, exclude) {
    alldata <- 0
    for (i in 0:(exps-1)) {

        if (i %in% exclude) {
            next
        } else {

            file <- paste(basedir, "/time_using_new_path_", i, ".log", sep='')

            data <- loadone(file)

            if (i == 0) {
                alldata <- data
            } else {
                alldata <- rbind(alldata, data)
            }
        }
    }
    return(alldata)
}

plotcdf <- function(outfile, data) {
    cdf_data <- ddply(data, c("arg_method"), transform, ecd = ecdf(count)(count))

    cbPalette <- c("#FF6688", "#FF6688", "#56B4E9", "#56B4E9", "#E69F00", "#E69F00")

    p3 <- ggplot(cdf_data,aes(x = count, y = ecd * 100,  col = factor(arg_method), linetype=factor(arg_method))) + 
        geom_line() +
        ylab("Fraction of flows") +
        xlab("Flow update time [ms]") +
        scale_x_continuous(limits = c(0, 1000), expand = c(0, 0)) +
        theme_bw() +
        theme(plot.margin=unit(x=c(1,6,1,1),units="mm"), legend.title=element_blank(), legend.key = element_rect(colour = "black"), legend.position="top", legend.margin=unit(-0.5, 'cm')) +
        scale_linetype_manual(name="Approach",values = c("solid", "dashed", "solid", "dashed","solid", "dashed")) +
        scale_colour_manual(name="Approach",values=cbPalette) 
    ggsave(p3, file=outfile, width=6.5, height=2.5)
}

i2_exclude = c()

i2data = loadall("time_new_path_i2", 1000, i2_exclude)
i2data$arg_method <- revalue(i2data$arg_method, c("cen"="I2 - Centralized", "ez"="I2 - ez-Segway"))

b4_exclude = c()

head(i2data)

b4data = loadall("time_new_path_b4", 1000, b4_exclude)
b4data$arg_method <- revalue(b4data$arg_method, c("cen"="B4 - Centralized", "ez"="B4 - ez-Segway"))

head(b4data)


alldata <- rbind(i2data, b4data)
alldata$arg_method <- factor(alldata$arg_method, levels=rev(levels(alldata$arg_method)))
plotcdf("cdf_flow_update.pdf", alldata)


