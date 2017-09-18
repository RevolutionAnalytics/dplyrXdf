## ---- echo=FALSE, message=FALSE, results="hide"--------------------------
knitr::opts_chunk$set(collapse=TRUE, comment="#>")
options(dplyr.print_min=5L, dplyr.print_max=5L)

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
flightsHd2 <- as_xdf(flightsCsv, "flights2")

as_data_frame(flightsHd2)

## ---- eval=FALSE---------------------------------------------------------
#  # establishes a Spark connection that can be shared with sparklyr
#  rxSparkConnect(..., interop="sparklyr")

## ---- eval=FALSE---------------------------------------------------------
#  cc <- rxSparkConnect(interop="sparklyr")
#  
#  sampleHiv <- RxHiveData(table="hivesampletable")
#  head(sampleHiv, 5)
#  #>   clientid querytime market deviceplatform devicemake devicemodel        state
#  #> 1        8  18:54:20  en-US        Android    Samsung    SCH-i500   California
#  #> 2       23  19:19:44  en-US        Android        HTC  Incredible Pennsylvania
#  #> 3       23  19:19:46  en-US        Android        HTC  Incredible Pennsylvania
#  #> 4       23  19:19:47  en-US        Android        HTC  Incredible Pennsylvania
#  #> 5       28  01:37:50  en-US        Android   Motorola     Droid X     Colorado
#  #>         country querydwelltime sessionid sessionpagevieworder
#  #> 1 United States      13.920401         0                    0
#  #> 2 United States             NA         0                    0
#  #> 3 United States       1.475742         0                    1
#  #> 4 United States       0.245968         0                    2
#  #> 5 United States      20.309534         1                    1
#  
#  sampleHiv %>%
#      filter(deviceplatform == "Android") %>%
#      group_by(devicemake) %>%
#      summarise(n=n()) %>%
#      arrange(desc(n)) %>%
#      head()
#  #> # Source:     lazy query [?? x 2]
#  #> # Database:   spark_connection
#  #> # Ordered by: desc(n)
#  #>     devicemake     n
#  #>          <chr> <dbl>
#  #> 1      Samsung 16244
#  #> 2           LG  7950
#  #> 3          HTC  2242
#  #> 4      Unknown  2133
#  #> 5     Motorola  1524
#  #> # ... with more rows

## ---- eval=FALSE---------------------------------------------------------
#  # this will create the composite Xdf 'samplehivetable' in the HDFS user directory
#  sampleXdf <- as_xdf(sampleHiv)
#  
#  sampleXdf %>%
#      filter(deviceplatform == "Android") %>%
#      group_by(devicemake) %>%
#      summarise(n=n()) %>%
#      arrange(desc(n)) %>%
#      head()
#  #>     devicemake     n
#  #> 1      Samsung 16244
#  #> 2           LG  7950
#  #> 3          HTC  2242
#  #> 4      Unknown  2133
#  #> 5     Motorola  1524

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

## ---- eval=FALSE---------------------------------------------------------
#  adlFs <- RxHdfsFileSystem(hostName="adl://mycluster.azuredatalakestore.net")
#  adlXdf <- RxXdfData("/path/to/file", fileSystem=adlFs)
#  
#  # adlXdf %>% verb1 %>% verb2 %>% ...

## ---- eval=FALSE---------------------------------------------------------
#  hdfs_dir("/", host="default")
#  #> Directory listing of /
#  #>  [1] "HdiNotebooks"             "HdiSamples"
#  #>  [3] "ams"                      "amshbase"
#  #>  [5] "app-logs"                 "apps"
#  #>  [7] "atshistory"               "cluster-info"
#  #>  [9] "custom-scriptaction-logs" "example"
#  #> [11] "hbase"                    "hdp"
#  #> [13] "hive"                     "mapred"
#  #> [15] "mr-history"               "tmp"
#  #> [17] "user"
#  
#  hdfs_dir("/", host="adl://mycluster.azuredatalakestore.net")
#  #> Directory listing of adl://mycluster.azuredatalakestore.net/
#  #> [1] "clusters"         "file31bc3ee52d32" "file31bc79a3de7"  "file5d301ef514e5"
#  #> [5] "folder1"          "tmp"              "user"

## ---- eval=FALSE---------------------------------------------------------
#  hdfs_dir("adl://mycluster.azuredatalakestore.net/")
#  #> Directory listing of adl://mycluster.azuredatalakestore.net/
#  #> [1] "clusters"         "file31bc3ee52d32" "file31bc79a3de7"  "file5d301ef514e5"
#  #> [5] "folder1"          "tmp"              "user"

## ---- eval=FALSE---------------------------------------------------------
#  hdfs_host()
#  #> [1] "adl://mycluster.azuredatalakestore.net"
#  
#  xdf <- RxXdfData("file", fileSystem=RxHdfsFileSystem(hostName="default"))
#  hdfs_host(xdf)
#  #> [1] "default"

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

