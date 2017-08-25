## ---- echo=FALSE, message=FALSE, results="hide"--------------------------
knitr::opts_chunk$set(collapse=TRUE, comment="#>")
options(dplyr.print_min=5L, dplyr.print_max=5L)

# to build this vignette as part of installation:
# - copy it to the /vignettes directory
# - ensure you have access to a Spark or Hadoop cluster (a remote connection via 
#   the RxSpark or RxHadoopMR compute context works)
# - run devtools::install(build_vignettes=TRUE)
dplyrXdf:::detectHdfsConnection()

## ------------------------------------------------------------------------
library(dplyrXdf)
library(nycflights13)

hd <- RxHdfsFileSystem()

# copy a data frame into an Xdf file in HDFS
flightsHd <- copy_to(hd, flights)

flightsHd

as_data_frame(flightsHd)

## ---- eval=FALSE---------------------------------------------------------
#  # same as above
#  flightsHd <- copy_to_hdfs(flights)

## ------------------------------------------------------------------------
flightsLocal <- compute(flightsHd)

flightsLocal

as_data_frame(flightsLocal)

## ------------------------------------------------------------------------
# create a csv file and upload it
write.csv(flights, "flights.csv", row.names=FALSE)
hdfs_upload("flights.csv", "/tmp")

## ------------------------------------------------------------------------
flightsCsv <- RxTextData("/tmp/flights.csv", fileSystem=RxHdfsFileSystem())
flightsHd2 <- as_xdf(flightsCsv, file="flights2")

as_data_frame(flightsHd2)

## ------------------------------------------------------------------------
# create a new directory
hdfs_dir_create("/tmp/mydata")

# check that it exists
hdfs_dir_exists("/tmp/mydata")

# copy files into the new directory
hdfs_file_copy("flights", "/tmp/mydata")

# create a new data source
flightsHd3 <- RxXdfData("/tmp/mydata/flights", fileSystem=RxHdfsFileSystem())

# read the data
names(flightsHd3)

## ------------------------------------------------------------------------
in_hdfs(flightsHd)

# also works with non-Revo data sources, like data frames
in_hdfs(iris)

in_hdfs(flights)

## ---- error=TRUE---------------------------------------------------------
# try to access a local Xdf file
names(flightsLocal)

local_exec(names(flightsLocal))

## ---- echo=FALSE, message=FALSE, results="hide"--------------------------
hdfs_dir_remove(c("flights", "flights2", "/tmp/mydata", "/tmp/flights.csv"))
clean_dplyrxdf_dir("hdfs")
clean_dplyrxdf_dir("native")

