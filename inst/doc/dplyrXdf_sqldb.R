## ---- echo=FALSE, message=FALSE, results="hide"--------------------------
knitr::opts_chunk$set(collapse=TRUE, comment="#>")
options(dplyr.print_min=5L, dplyr.print_max=5L)

cc <- rxSetComputeContext("local")

db <- DBI::dbConnect(odbc::odbc(), .connection_string=connStr)
if(DBI::dbExistsTable(db, "flights")) DBI::dbRemoveTable(db, "flights")
if(DBI::dbExistsTable(db, "flightsQry")) DBI::dbRemoveTable(db, "flightsQry")

## ------------------------------------------------------------------------
library(dplyrXdf) # also loads dplyr
library(nycflights13)

# copy the flights dataset to SQL Server
flightsSql <- RxSqlServerData("flights", connectionString=connStr)
flightsHd <- copy_to(flightsSql, flights)

## ------------------------------------------------------------------------
flightsQry <- flightsSql %>%
    filter(month > 6) %>%
    group_by(carrier) %>%
    summarise(avg_delay=mean(arr_delay))

flightsQry

## ------------------------------------------------------------------------
compute(flightsQry)

compute(flightsQry, name="flightsQry", temporary=FALSE)

## ------------------------------------------------------------------------
# import a table to the local machine
flightsXdf <- as_xdf(flightsSql)

flightsXdf %>%
    filter(month > 6) %>%
    group_by(carrier) %>%
    summarise(avg_delay=mean(arr_delay)) %>%
    head

## ---- echo=FALSE, message=FALSE, results="hide"--------------------------
delete_xdf(flightsXdf)
DBI::dbRemoveTable(db, "flightsQry")
DBI::dbRemoveTable(db, "flights")
DBI::dbDisconnect(db)
rxSetComputeContext(cc)

