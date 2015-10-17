## ----, echo = FALSE, message = FALSE-------------------------------------
knitr::opts_chunk$set(collapse = TRUE, comment = "#>")
options(dplyr.print_min = 5L, dplyr.print_max = 5L)
library(dplyrXdf)

## ------------------------------------------------------------------------
library(dplyrXdf)  # also loads dplyr
library(nycflights13)

# write the data as an xdf file
flightsXdf <- rxDataFrameToXdf(flights, "flights.xdf", overwrite=TRUE)

## ------------------------------------------------------------------------
# a simple transformation
flightsMut <- mutate(flightsXdf, delay = (dep_delay + arr_delay)/2)
head(flightsMut)

# a more complex transformation involving a transformFunc
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

## ----, eval=FALSE--------------------------------------------------------
#  mutate(flightsXdf, delayHrs=delay/60,
#      .transformArgs=list(
#          transformFunc=function(varlist) with(varlist, {
#              delay <- (dep_delay + arr_delay)/2
#              list(delay=delay)
#          }),
#          transformVars=c("dep_delay", "arr_delay")
#      )
#  )
#  #> Error in doTryCatch(return(expr), name, parentenv, handler) :
#  #>   Error in executing R code: object 'delay' not found

## ------------------------------------------------------------------------
flightsSmry <- flightsXdf %>%
    group_by(carrier) %>%
    summarise(sumdist=sum(dist_km),
              .rxArgs=list(rowSelection=month > 6,
                           transforms=list(dist_km=distance * 1.6093))
    )
head(flightsSmry)

## ----, eval=FALSE--------------------------------------------------------
#  datasrc %>%
#      mutate(xwt=sum(x*wt)) %>%
#      summarise(xwt=sum(xwt), wt=sum(wt)) %>%
#      mutate(weightedMean=xwt/wt)

## ----, eval=FALSE--------------------------------------------------------
#  factorise(data, x1, x2, ...)

## ------------------------------------------------------------------------
# fit a regression model by carrier, using rxLinMod
flightsMods <- flightsXdf %>%
    group_by(carrier) %>%
    doXdf(model=rxLinMod(arr_delay ~ dep_delay + hour, data=.))

flightsMods$model[[1]]

## ------------------------------------------------------------------------
planesXdf <- rxDataFrameToXdf(planes, "planes.xdf", overwrite=TRUE)

# same as semi_join(flights, planes, by="tailnum")
flightsSemi <- inner_join(flightsXdf,
                          select(planesXdf, tailnum) %>% distinct,
                          by="tailnum")
head(flightsSemi)

## ------------------------------------------------------------------------
# same as anti_join(flights, planes, by="tailnum")
flightsAnti <- left_join(flightsXdf,
                         transmute(planesXdf, tailnum, pl=rep(1, .rxNumRows)) %>%
                            distinct,
                         by="tailnum") %>% filter(is.na(pl))
head(flightsAnti)

## ------------------------------------------------------------------------
# same as union(flightsXdf, flightsXdf)
flightsUnion <- rxMerge(flightsXdf, flightsXdf, outFile="flightsUnion.xdf",
                        type="union", overwrite=TRUE) %>% distinct
nrow(flightsXdf)
nrow(flightsUnion)  # same as nrow(flightsXdf)

