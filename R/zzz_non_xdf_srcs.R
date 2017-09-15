#' Methods for non-Xdf RevoScaleR data sources
#'
#' Despite the name, dplyrXdf can work with any RevoScaleR data source, not just Xdf files. These are the verbs that can accept as inputs non-Xdf data sources.
#'
#' @details
#' There are a number of ways in which dplyrXdf verbs handle non-Xdf data sources:
#' \enumerate{
#'   \item File data sources, including delimited text (\code{\link{RxTextData}}), Avro (\code{RxAvroData}), SAS datasets (\code{\link{RxSasData}}) and SPSS datasets (\code{\link{RxSpssData}}), are generally handled inline, ie, they are read and processed much like an Xdf file would be.
#'   \item ODBC data sources, including \code{\link{RxOdbcData}}, \code{\link{RxSqlServerData}} and \code{\link{RxTeradata}} usually represent tables in a SQL database. These data sources are converted into a dplyr tbl, which is then processed by dplyr (\emph{not} dplyrXdf) in-database.
#'  \item A Hive table (\code{\link{RxHiveData}}) in HDFS is turned into a sparklyr tbl and processed by sparklyr.
#'  \item Other data sources are converted to Xdf format and then processed. The main difference between this and 1) above is that the data is written to an Xdf file first, before being transformed; this is less efficient due to the extra I/O involved.
#' }
#'
#' Running a pipeline in-database requires that a suitable dplyr backend for the DBMS in question be available. There are backends for many popular commercial and open-source DBMSes, including SQL Server, PostgreSQL and Apache Hive; a Teradata backend is not yet available, but is in development at the time of writing (September 2017). For more information on how dplyr executes pipelines against database sources, see the \href{http://dbplyr.tidyverse.org/articles/dbplyr.html}{database vignette} on the Tidyverse website. Using this functionality does require you to install a few additional packages, namely odbc and dbplyr (and their dependencies).
#'
#' Similarly, running a pipeline on a Hive data source with sparklyr requires that package to be installed. You must also be running on the edge node of a Spark cluster (not on a remote client, and not on a Hadoop cluster). For best results it's recommended that you should use \code{\link{rxSparkConnect(interop="sparklyr")}} to set the compute context; otherwise, dplyrXdf will open a separate sparklyr connection using \code{spark_connect(master="yarn-client")}, which may or may not be appropriate for your cluster. More information about sparklyr is available on the \href{https://spark.rstudio.com/}{Rstudio Sparklyr site}.
#'
#' While running a pipeline in-database or in-Spark can often be much more efficient than running the code locally, there are a few points to be aware of.
#' \itemize{
#'   \item For in-database pipelines, each pipeline will open a separate connection to the database; this connection remains open while any tbl objects related to the pipeline still exist. This is unlikely to cause problems for interactive use, but may do so if the code is reused for batch jobs, eg as part of a predictive web service.
#'   \item Which verbs are supported will vary by backend. For example, \code{factorise} and \code{do_xdf} are meant for Xdf files, and will probably fail inside a database.
#'   \item The Xdf-specific arguments \code{.outFile} and \code{\link{.rxArgs}} are not available in-database or in sparklyr. In particular, this means you cannot use a \link[=rxTransform]{transformFunc} to carry out arbitrary transformations on the data.
#' }
#' @aliases non_xdf
#' @rdname nonxdf
#' @name nonxdf
NULL


#' @rdname arrange
#' @export
arrange.RxDataSource <- function(.data, ...)
{
    arrange(convertSrc(.data), ...)
}


#' @rdname distinct
#' @export
distinct.RxDataSource <- function(.data, ...)
{
    distinct(convertSrc(.data), ...)
}


#' @rdname do
#' @export
do.RxDataSource <- function(.data, ...)
{
    do(convertSrc(.data), ...)
}


#' @rdname do
#' @export
do_xdf.RxDataSource <- function(.data, ...)
{
    do_xdf(convertSrc(.data), ...)
}


#' @rdname factorise
#' @export
factorise.RxDataSource <- function(.data, ...)
{
    factorise(convertSrc(.data), ...)
}


#' @rdname filter
#' @export
filter.RxDataSource <- function(.data, ...)
{
    filter(convertSrc(.data), ...)
}


#' @rdname group_by
#' @export
group_by.RxDataSource <- function(.data, ...)
{
    group_by(convertSrc(.data), ...)
}


#' @rdname mutate
#' @export
mutate.RxDataSource <- function(.data, ...)
{
    mutate(convertSrc(.data), ...)
}


#' @rdname rename
#' @export
rename.RxDataSource <- function(.data, ...)
{
    rename(convertSrc(.data), ...)
}


#' @rdname select
#' @export
select.RxDataSource <- function(.data, ...)
{
    select(convertSrc(.data), ...)
}


#' @rdname summarise
#' @export
summarise.RxDataSource <- function(.data, ...)
{
    summarise(convertSrc(.data), ...)
}


#' @rdname subset
#' @export
subset.RxDataSource <- function(.data, subset, select, ...)
{
    sub <- rlang::enquo(subset)
    sel <- rlang::enquo(select)
    if(rlang::quo_is_missing(sel))
        convertSrc(.data) %>% filter(!!sub)
    else convertSrc(.data) %>% filter(!!sub) %>% select(!!sel)
}


#' @rdname mutate
#' @export
transmute.RxDataSource <- function(.data, ...)
{
    transmute(convertSrc(.data), ...)
}


#' @rdname join
#' @export
left_join.RxDataSource <- function(x, ...)
{
    left_join(convertSrc(x), ...)
}


#' @rdname join
#' @export
right_join.RxDataSource <- function(x, ...)
{
    right_join(convertSrc(x), ...)
}


#' @rdname join
#' @export
full_join.RxDataSource <- function(x, ...)
{
    full_join(convertSrc(x), ...)
}


#' @rdname join
#' @export
inner_join.RxDataSource <- function(x, ...)
{
    inner_join(convertSrc(x), ...)
}


#' @rdname join
#' @export
anti_join.RxDataSource <- function(x, ...)
{
    anti_join(convertSrc(x), ...)
}


#' @rdname join
#' @export
semi_join.RxDataSource <- function(x, ...)
{
    semi_join(convertSrc(x), ...)
}


#' @rdname setops
#' @export
intersect.RxDataSource <- function(x, ...)
{
    intersect(convertSrc(x), ...)
}


#' @rdname setops
#' @export
setdiff.RxDataSource <- function(x, ...)
{
    setdiff(convertSrc(x), ...)
}


#' @rdname setops
#' @export
setequal.RxDataSource <- function(x, ...)
{
    setequal(convertSrc(x), ...)
}


#' @rdname setops
#' @export
union.RxDataSource <- function(x, ...)
{
    union(convertSrc(x), ...)
}


#' @rdname setops
#' @export
union_all.RxDataSource <- function(x, ...)
{
    union_all(convertSrc(x), ...)
}


convertSrc <- function(.data)
{
    UseMethod("convertSrc")
}


# convert a SQL Server data source to a dplyr tbl
convertSrc.RxSqlServerData <- function(.data)
{
    if(is.null(.data@table))
        stop("data source must be a table (not a SQL query)", call.=FALSE)

    if(!requireNamespace("odbc", quietly=TRUE))
        stop("odbc package required to use dplyrXdf with RxSqlServerData sources", call.=FALSE)

    db <- DBI::dbConnect(odbc::odbc(), .connection_string=.data@connectionString)
    tbl(db, .data@table)
}


# convert an ODBC or Teradata data source to a dplyr tbl
convertSrc.RxOdbcData <- function(.data)
{
    if(is.null(.data@table))
        stop("data source must be a table (not a SQL query)", call.=FALSE)

    if(!requireNamespace("odbc", quietly=TRUE))
        stop("odbc package required to use dplyrXdf with RxOdbcData and RxTeradata sources", call.=FALSE)

    db <- DBI::dbConnect(odbc::odbc(),
        server=.data@server,
        database=.data@databaseName,
        uid=.data@user, pwd=.data@password,
        .connection_string=.data@connectionString)
    tbl(db, .data@table)
}


# convert a Hive data source to a sparklyr tbl (if possible)
convertSrc.RxHiveData <- function(.data)
{
    if(is.null(.data@table))
        stop("data source must be a table (not a SQL query)", call.=FALSE)

    # if remote, import to xdf (no failsafe way to create separate sparklyr connection)
    if(isRemoteHdfsClient())
    {
        file <- tbl_xdf(fileSystem=RxHdfsFileSystem(hostName=hdfs_host()))@file
        message("Spark session is remote; importing Hive table to Xdf file ", file)
        return(as_xdf(.data, file=file))
    }

    if(!requireNamespace("sparklyr", quietly=TRUE))
        stop("sparklyr package required to use dplyrXdf with local RxHiveData sources", call.=FALSE)

    # if Spark CC and interop feature present, use it
    sc <- try(rxGetSparklyrConnection(), silent=TRUE)

    # otherwise create a separate sparklyr connection
    if(inherits(sc, "try-error"))
        sc <- sparklyr::spark_connect(master="yarn-client")

    tbl(sc, .data@table)
}


# for other data sources: import to xdf in dplyrXdf temp directory
convertSrc.RxDataSource <- function(.data)
{
    file <- tbl_xdf(fileSystem=rxGetFileSystem(.data))@file
    message("Importing data source to Xdf file ", file)
    as_xdf(.data, file=file)
}
