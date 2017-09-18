#' Download a dataset to the local machine
#'
#' @param x An Xdf data source object.
#' @param as_data_frame For the \code{RxXdfData} methods: should the downloaded data be converted to a data frame, or left as an Xdf file?
#' @param name For the \code{RxDataSource} methods: the name of the Xdf file to create. Defaults to a temporary filename in the dplyrXdf working directory.
#' @param ... If the output is to be a data frame, further arguments to the \code{as.data.frame} method.
#'
#' @details
#' RevoScaleR does not have an exact analogue of the dplyr concept of a src, and because of this, the dplyrXdf implementations of \code{collect} and \code{compute} are somewhat different. In dplyrXdf, these functions serve two related, overlapping purposes:
#' \itemize{
#'    \item Copy an arbitrary data source from a backend to an Xdf file or data frame. The data source can be any (non-Xdf) RevoScaleR data source, such as a SQL Server table (class \code{RxSqlServerData}).
#'    \item Download an Xdf file from a remote filesystem, such as the HDFS filesystem of a Hadoop or Spark cluster.
#' }
#'
#' The code will handle both the cases where you are logged into the edge node of a Hadoop/Spark cluster, and if you are a remote client. For the latter case, the downloading is a two-stage process: the data is first transferred from HDFS to the native filesystem of the edge node, and then downloaded from the edge node to the client.
#'
#' If you want to look at the first few rows of a small Xdf file in HDFS, it may be faster to use \code{compute}) to copy the entire file to the native filesystem, and then run \code{head}, than to run \code{head} on the original. This is due to RevoScaleR overhead in Spark and Hadoop.
#'
#' @return
#' For the \code{RxDataSource} methods, \code{collect} returns a data frame, and \code{compute} returns a tbl_xdf data source. For the \code{RxXdfData} methods, either a data frame or tbl_xdf based on the \code{as_data_frame} argument.
#'
#' @seealso
#' \code{\link{as_xdf}}, \code{\link{as_data_frame}}, \code{\link{copy_to}}, \code{\link[dplyr]{compute}} in package dplyr
#' @aliases collect, compute
#'
#' @examples
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#'
#' # all of these return a data frame (or a tbl_df) for input in the native filesystem
#' as.data.frame(mtx)
#' as_data_frame(mtx)  # returns a tbl_df
#' collect(mtx)
#' compute(mtx)
#'
#' # collect and compute are meant for downloading data from remote backends
#' \dontrun{
#' # downloading from a database
#' connStr <- "SERVER=hostname;DATABASE=RevoTestDB;TRUSTED_CONNECTION=yes"
#' mtdb <- RxSqlServerData("mtcars", connectionString=connStr)
#' copy_to(mtdb, mtcars)
#' as.data.frame(mtdb)
#' collect(mtdb)  # returns a data frame
#' compute(mtdb)  # returns a tbl_xdf
#' 
#' # downloading from HDFS
#' mtc <- copy_to_hdfs(mtcars)
#' as.data.frame(mtc)
#' collect(mtc)  # returns a data frame
#' compute(mtc)  # returns a tbl_xdf
#' }
#' @aliases collect compute
#' @rdname compute
#' @export
collect.RxXdfData <- function(x, as_data_frame=TRUE, ...)
{
    if(in_hdfs(x))
    {
        # copy from HDFS to native filesystem
        composite <- is_composite_xdf(x)
        file <- file.path(get_dplyrxdf_dir("native"), basename(x@file))
        localXdf <- tbl_xdf(file=file, fileSystem=RxNativeFileSystem(), createCompositeSet=composite)

        hdfs_download(x@file, localXdf@file, overwrite=TRUE, host=x@fileSystem$hostName)
    }
    else localXdf <- x

    if(as_data_frame)
    {
        if(in_hdfs(x))
            on.exit(delete_xdf(localXdf))
        as.data.frame(localXdf, ...)
    }
    else localXdf
}


#' @rdname compute
#' @export
compute.RxXdfData <- function(x, as_data_frame=!in_hdfs(x), ...)
{
    if(in_hdfs(x))
    {
        # copy from HDFS to native filesystem
        composite <- is_composite_xdf(x)
        file <- file.path(get_dplyrxdf_dir("native"), basename(x@file))
        localXdf <- tbl_xdf(file=file, fileSystem=RxNativeFileSystem(), createCompositeSet=composite)

        hdfs_download(x@file, localXdf@file, overwrite=TRUE, host=x@fileSystem$hostName)
    }
    else localXdf <- x

    if(as_data_frame)
    {
        if(in_hdfs(x))
            on.exit(delete_xdf(localXdf))
        as.data.frame(localXdf, ...)
    }
    else localXdf
}


#' @rdname compute
#' @export
compute.RxDataSource <- function(x, name=NULL, ...)
{
    if(is.null(name))
    {
        name <- if(inherits(x, c("RxFileData", "RxOrcData", "RxParquetData")))
            x@file
        else if(inherits(x, c("RxOdbcData", "RxTeradata")) && !is.null(x@table))
            x@table
        else URLencode(deparse(substitute(x)), reserved=TRUE)
    }

    fs <- if(in_hdfs(x)) "hdfs" else "native"
    file <- file.path(get_dplyrxdf_dir(fs), basename(name))
    as(as_xdf(x, file, ...), "tbl_xdf")
}


#' @rdname compute
#' @export
collect.RxDataSource <- function(x, ...)
{
    # if data in HDFS: import to xdf, rely on as.data.frame.RxXdfData to download and convert to df
    if(in_hdfs(x))
    {
        x <- compute(x)
        on.exit(delete_xdf(x))
    }

    as.data.frame(x, ...)
}


