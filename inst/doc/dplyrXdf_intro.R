## ---- echo = FALSE, message = FALSE--------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>")
options(dplyr.print_min = 5L, dplyr.print_max = 5L)
library(dplyrXdf)

## ------------------------------------------------------------------------
library(dplyrXdf)  # also loads dplyr
library(nycflights13)

# write the data as an xdf file
flightsXdf <- rxDataFrameToXdf(flights, "flights.xdf", overwrite=TRUE)

## ------------------------------------------------------------------------
# select the rows
flights_rx1 <- rxDataStep(flightsXdf, outFile="flights_rx1.xdf",
                          rowSelection=month <= 6 & year == 2013,
                          overwrite=TRUE)

# variable transformations
flights_rx2 <- rxDataStep(flights_rx1, outFile="flights_rx2.xdf",
                          transforms=list(dist_km=distance*1.6093,
                                          delay=(arr_delay + dep_delay)/2),
                          overwrite=TRUE)

# convert carrier into a factor variable (or rxSummary will complain)
flights_rx3 <- rxFactors(flights_rx2, factorInfo="carrier",
                         outFile="flights_rx3.xdf", overwrite=TRUE)

# use rxSummary to get the summary table(s) (could also use rxCube twice)
flights_rx4 <- rxSummary(~delay:carrier + dist_km:carrier, data=flights_rx3,
                         summaryStats=c("mean", "sum"))

# extract the desired tables from the rxSummary output
flights_rx4_1 <- flights_rx4$categorical[[1]][c("carrier", "Means")]
names(flights_rx4_1)[2] <- "mean_delay"

flights_rx4_2 <- flights_rx4$categorical[[2]][c("carrier", "Sum")]
names(flights_rx4_2)[2] <- "sum_dist"

# merge the tables together
flights_rx5 <- merge(flights_rx4_1, flights_rx4_2, by="carrier", all=TRUE)

# sort the results
flights_rx5 <- flights_rx5[order(flights_rx5$mean_delay, decreasing=TRUE), ]

head(flights_rx5)

## ------------------------------------------------------------------------
flightsSmry <- flightsXdf %>%
    filter(month <= 6, year == 2013) %>%
    mutate(dist_km=distance*1.6093, delay=(arr_delay + dep_delay)/2) %>%
    group_by(carrier) %>%
    summarise(mean_delay=mean(delay), sum_dist=sum(dist_km)) %>%
    arrange(desc(mean_delay))

head(flightsSmry)

## ------------------------------------------------------------------------
airportsXdf <- rxDataFrameToXdf(airports, "airports.xdf", overwrite=TRUE)
flightsJoin <- left_join(
    flightsXdf %>% select(year:day, hour, origin, dest, tailnum, carrier),
    airportsXdf,
    by=c("dest"="faa"))
head(flightsJoin)

## ---- eval=FALSE---------------------------------------------------------
#  # pipeline 1
#  output1 <- flightsXdf %>%
#      mutate(delay=(arr_delay + dep_delay)/2)
#  
#  # use the output from pipeline 1
#  output2 <- output1 %>%
#      group_by(airline) %>%
#      summarise(delay=mean(delay))
#  
#  # reuse the output from pipeline 1 -- WRONG
#  output3 <- output1 %>%
#      group_by(dest) %>%
#      summarise(delay=mean(delay))

## ---- eval=FALSE---------------------------------------------------------
#  # pipeline 1 -- use persist to save the data
#  output1 <- flightsXdf %>%
#      mutate(delay=(arr_delay + dep_delay)/2) %>% persist("output1.xdf")
#  
#  # use the output from pipeline 1
#  output2 <- output1 %>%
#      group_by(carrier) %>%
#      summarise(delay=mean(delay))
#  
#  # reuse the output from pipeline 1 -- this works as expected
#  output3 <- output1 %>%
#      group_by(dest) %>%
#      summarise(delay=mean(delay))

## ------------------------------------------------------------------------
flightsTbl <- tbl(flightsXdf)
flightsTbl

