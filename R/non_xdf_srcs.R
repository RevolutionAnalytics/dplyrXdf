#' @export
arrange.RxDataSource <- function(.data, ...)
{
    arrange(convertSrc(.data), ...)
}


#' @export
distinct.RxDataSource <- function(.data, ...)
{
    distinct(convertSrc(.data), ...)
}


#' @export
do.RxDataSource <- function(.data, ...)
{
    do(convertSrc(.data), ...)
}


#' @export
filter.RxDataSource <- function(.data, ...)
{
    filter(convertSrc(.data), ...)
}


#' @export
group_by.RxDataSource <- function(.data, ...)
{
    group_by(convertSrc(.data), ...)
}


#' @export
mutate.RxDataSource <- function(.data, ...)
{
    mutate(convertSrc(.data), ...)
}


#' @export
rename.RxDataSource <- function(.data, ...)
{
    rename(convertSrc(.data), ...)
}


#' @export
select.RxDataSource <- function(.data, ...)
{
    select(convertSrc(.data), ...)
}


#' @export
summarise.RxDataSource <- function(.data, ...)
{
    summarise(convertSrc(.data), ...)
}


#' @export
subset.RxDataSource <- function(.data, subset, select, ...)
{
    sub <- rlang::enquo(subset)
    sel <- rlang::enquo(select)
    if(rlang::quo_is_missing(sel))
        convertSrc(.data) %>% filter(!!sub)
    else convertSrc(.data) %>% filter(!!sub) %>% select(!!sel)
}


#' @export
transmute.RxDataSource <- function(.data, ...)
{
    transmute(convertSrc(.data), ...)
}


#' @export
left_join.RxDataSource <- function(x, ...)
{
    left_join(convertSrc(x), ...)
}


#' @export
right_join.RxDataSource <- function(x, ...)
{
    right_join(convertSrc(x), ...)
}


#' @export
full_join.RxDataSource <- function(x, ...)
{
    full_join(convertSrc(x), ...)
}


#' @export
inner_join.RxDataSource <- function(x, ...)
{
    inner_join(convertSrc(x), ...)
}


#' @export
anti_join.RxDataSource <- function(x, ...)
{
    anti_join(convertSrc(x), ...)
}


#' @export
semi_join.RxDataSource <- function(x, ...)
{
    semi_join(convertSrc(x), ...)
}


#' @export
union.RxDataSource <- function(x, ...)
{
    union(convertSrc(x), ...)
}


#' @export
union_all.RxDataSource <- function(x, ...)
{
    union_all(convertSrc(x), ...)
}


convertSrc <- function(.data)
{
    UseMethod("convertSrc")
}


# convert a SQL Server data source to a dplyr src
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
