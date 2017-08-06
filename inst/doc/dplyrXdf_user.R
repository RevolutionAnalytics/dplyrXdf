## ---- echo = FALSE, message = FALSE--------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>")
options(dplyr.print_min = 5L, dplyr.print_max = 5L)

## ------------------------------------------------------------------------
library(dplyrXdf) # also loads dplyr
library(nycflights13)

flightsXdf <- as_xdf(flights)
tbl_vars(flightsXdf)

## ------------------------------------------------------------------------
# create a RevoScaleR text data source pointing to a csv file
write.csv(flights, "flights.csv", row.names=FALSE)
flightsCsv <- RxTextData("flights.csv")

flightsXdf2 <- as_xdf(flights, file="flights2.xdf")
tbl_vars(flightsXdf2)

## ---- echo=FALSE, message=FALSE, results="hide"--------------------------
file.remove("flights.csv")
delete_xdf(flightsXdf2)

## ---- eval=FALSE---------------------------------------------------------
#  txt <- RxTextData("path/to/file.txt")
#  txtTbl <- as(txt, "tbl_xdf")

## ---- eval=FALSE---------------------------------------------------------
#  # pipeline 1
#  output1 <- flightsXdf %>%
#      mutate(delay=(arr_delay + dep_delay)/2)
#  
#  # use the output from pipeline 1
#  output2 <- output1 %>%
#      group_by(carrier) %>%
#      summarise(delay=mean(delay))
#  
#  # reuse the output from pipeline 1 -- WRONG
#  output3 <- output1 %>%
#      group_by(dest) %>%
#      summarise(delay=mean(delay))

## ------------------------------------------------------------------------
# pipeline 1 -- use .outFile to save the data
output1 <- flightsXdf %>%
    mutate(delay=(arr_delay + dep_delay)/2, .outFile="output1.xdf")

# use the output from pipeline 1
output2 <- output1 %>%
    group_by(carrier) %>%
    summarise(delay=mean(delay))

# reuse the output from pipeline 1 -- this works as expected
output3 <- output1 %>%
    group_by(dest) %>%
    summarise(delay=mean(delay))

## ---- eval=FALSE---------------------------------------------------------
#  # pipeline 1 -- use persist to save the data
#  output1 <- flightsXdf %>%
#      mutate(delay=(arr_delay + dep_delay)/2) %>% persist("output1_persist.xdf")
#  
#  # use the output from pipeline 1
#  output2 <- output1 %>%
#      group_by(carrier) %>%
#      summarise(delay=mean(delay))
#  
#  # reuse the output from pipeline 1 -- this also works as expected
#  output3 <- output1 %>%
#      group_by(dest) %>%
#      summarise(delay=mean(delay))

## ------------------------------------------------------------------------
outputXdf <- as_xdf(output3)

output3

# no longer a tbl_xdf
outputXdf

## ------------------------------------------------------------------------
output3 <- move_xdf(output3, "d:/data/output3.xdf")
output3

## ------------------------------------------------------------------------
compositeOutput3 <- as_composite_xdf(output3)
compositeOutput3

## ---- echo=FALSE, message=FALSE, results="hide"--------------------------
delete_xdf(output3)
delete_xdf(compositeOutput3)

## ------------------------------------------------------------------------
subset(flights, month <= 6 & day == 1, c(dep_time, dep_delay, carrier))

## ------------------------------------------------------------------------
flightsXdfSub <- subset(flightsXdf, month <= 6 & day == 1, c(dep_time, dep_delay, carrier))
class(flightsXdfSub)
head(flightsXdfSub)

## ------------------------------------------------------------------------
# subset, transform and summarise in the one step
flightsSubsetSmry <- flightsXdf %>% group_by(day) %>%
    summarise(delay=mean(delay), n=n(),
        .rxArgs=list(
            transforms=list(delay=(dep_delay + arr_delay)/2),
            rowSelection=carrier == "UA"
        )
    )
head(flightsSubsetSmry)

# a complex transformation involving a transformFunc
flightsTrans <- transmute(flightsXdf, 
    .rxArgs=list(
        transformFunc=function(varlist) with(varlist, {
            delay <- (dep_delay + arr_delay)/2
            date <- as.Date(sprintf("%d-%02d-%02d", year, month, day))
            weekday <- weekdays(date)
            weekendDelay <- ifelse(weekday %in% c("Saturday", "Sunday"),
                                   delay, NA)
            list(delay=delay, weekday=weekday, weekendDelay=weekendDelay)
        })
    )
)
head(flightsTrans)

# fit a model using open source R, and then score the training dataset
# we pass the model object via transformObjects, and the package to load
# via transformPackages
library(rpart)
flightsModel <- rpart(arr_delay ~ dep_delay + carrier + hour, data=flights)

flightsScores <- transmute(flightsXdf,
    pred=predict(model, data.frame(dep_delay, carrier, hour)),
    .rxArgs=list(
        transformObjects=list(model=flightsModel),
        transformPackages="rpart"
    )
)
head(flightsScores)

## ---- eval=FALSE---------------------------------------------------------
#  datasrc %>%
#      mutate(xwt=sum(x*wt)) %>%
#      summarise(xwt=sum(xwt), wt=sum(wt)) %>%
#      mutate(weightedMean=xwt/wt)

## ---- eval=FALSE---------------------------------------------------------
#  datasrc %>%
#      summarise(weightedMean=mean(x), .rxArgs=list(pweight="wt"))

## ---- eval=FALSE---------------------------------------------------------
#  factorise(data, x1, x2, ...)

## ---- eval=FALSE---------------------------------------------------------
#  factorise(data, x1=c("a", "b", "c"))

## ------------------------------------------------------------------------
# fit a regression model by carrier, using rxLinMod
flightsMods <- flightsXdf %>%
    group_by(carrier) %>%
    do_xdf(model=rxLinMod(arr_delay ~ dep_delay + hour, data=.))

flightsMods$model[[1]]

## ------------------------------------------------------------------------
get_dplyrxdf_dir()

## ---- eval=FALSE---------------------------------------------------------
#  # set the tbl directory to a network drive (on Windows)
#  set_dplyrxdf_dir("n:/Rtemp")

