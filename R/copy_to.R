#' Upload a dataset to a remote backend
#'
#' @param dest The destination source: either a RevoScaleR data source object, or a filesystem object of class \code{\link{RxHdfsFileSystem}}.
#' @param df A dataset. For the \code{RxDataSource} method, this can be any RevoScaleR data source object, presumably of a different class to the destination. For the \code{RxHdfsFileSystem} method, this can be the filename of an Xdf file, a RevoScaleR data source, or anything that can be coerced to a data frame.
#' @param name The filename, optionally including the path, for the uploaded Xdf file. The default upload location is the user's home directory (\code{user/<username>}) in the filesystem pointed to by \code{dest}. Not used for the \code{RxDataSource} method.
#' @param host,port HDFS hostname and port number to connect to. You should need to set these only if you have an attached Azure Data Lake Store that you are accessing via HDFS.
#' @param ... Further arguments to lower-level functions; see below.
#'
#' @details
#' RevoScaleR does not have an exact analogue of the dplyr concept of a src, and because of this, the dplyrXdf implementation of \code{copy_to} is somewhat different. In dplyrXdf, the function serves two related, overlapping purposes:
#' \itemize{
#'    \item First, it can be used to copy a dataset to a different \emph{format}, for example from an Xdf file to a SQL Server database. To do this, \code{dest} should be a data source object of the target class (\code{RxSqlServerData} for SQL Server), specifying the name/location of the copied data.
#'    \item Second, it can be used to upload a dataset to a different \code{filesystem}, such as the HDFS filesystem of a Hadoop or Spark cluster. The dataset will be saved in Xdf format. For this, \code{dest} should be a \code{RxHdfsFileSystem} object.
#' }
#' 
#' The \code{copy_to_hdfs} function is a simple wrapper to the HDFS upload method that avoids having to create an explicit filesystem object. Its arguments other than \code{host} and \code{port} are simply passed as-is to \code{copy_to.RxHdfsFileSystem}.
#'
#' The method for uploading to HDFS can handle both the cases where you are logged into the edge node of a Hadoop/Spark cluster, and where you are a remote client. For the latter case, the uploading is a two-stage process: the data is first transferred to the native filesystem of the edge node, and then copied from the edge node into HDFS. Similarly, it can handle uploading both to the host HDFS filesystem, and to an attached Azure Data Lake Store. If \code{dest} points to an ADLS host, the file will be uploaded there. You can override this by supplying an explicit an explicit URI for the uploaded file, in the form \code{adl://azure.host.name/path}. The name for the host HDFS filesystem is \code{adl://host/}.
#'
#' For the HDFS upload method, any arguments in \code{...} are passed to \code{\link{hdfs_upload}}, and ultimately to the Hadoop \code{fs -copytoLocal} command. For the data source copy method, arguments in \code{...} are passed to \code{\link{rxDataStep}}.
#'
#' \code{copy_to} is meant for copying \emph{datasets} to different backends. If you are simply copying a file to HDFS, consider using \code{hdfs_upload}; or if you are copying an Xdf file to a different location in the same filesystem, use \code{copy_xdf} or \code{\link{file.copy}}.
#'
#' @section Note on composite Xdf:
#' There are actually two kinds of Xdf files: standard and \emph{composite}. A composite Xdf file is a directory containing multiple data and metadata files, which the RevoScaleR functions treat as a single dataset. Xdf files in HDFS must be composite in order to work properly; \code{copy_to} will convert an existing Xdf file into composite, if it's not already in that format. Non-Xdf datasets (data frames and other RevoScaleR data sources, such as text files) will similarly be uploaded as composite.
#'
#' @return
#' An Xdf data source object pointing to the uploaded data.
#'
#' @seealso
#' \code{\link{rxHadoopCopyFromClient}}, \code{\link{rxHadoopCopyFromLocal}},
#' \code{\link{collect}} and \code{\link{compute}} for downloading data from HDFS,
#' \code{\link{as_xdf}}, \code{\link{as_composite_xdf}}
#'
#' @examples
#' \dontrun{
#' # copy a data frame to SQL Server
#' connStr <- "SERVER=hostname;DATABASE=RevoTestDB;TRUSTED_CONNECTION=yes"
#' mtdb <- RxSqlServerData("mtcars", connectionString=connString)
#' copy_to(mtdb, mtcars)
#'
#' # copy an Xdf file to SQL Server: will overwrite any existing table with the same name
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#' copy_to(mtdb, mtx)
#'
#' # copy a data frame to HDFS
#' hd <- RxHdfsFileSystem()
#' mth <- copy_to(hd, mtcars)
#' # assign a new filename on copy
#' mth2 <- copy_to(hd, mtcars, "mtcars_2")
#'
#' # copy an Xdf file to HDFS
#' mth3 <- copy_to(hd, mtx, "mtcars_3")
#'
#' # same as copy_to(hd, ...)
#' delete_xdf(mth)
#' copy_to_hdfs(mtcars)
#'
#' # copying to attached ADLS storage
#' copy_to_hdfs(mtcars, host="adl://adls.host.name")
#' }
#' @aliases copy_to
#' @rdname copy_to
#' @export
copy_to.RxDataSource <- function(dest, df, ...)
{
    srcInHdfs <- in_hdfs(df)
    destInHdfs <- in_hdfs(dest)

    if(srcInHdfs && !destInHdfs) # copying to local database from HDFS xdf/Orc/Parquet/Hive
    {
        if(!inherits(df, "RxXdfData")) # import data to xdf file if necessary
            df <- compute(df)

        # download the xdf file to local
        df <- compute(df)
        on.exit(delete_xdf(df))
    }

    if(!srcInHdfs && destInHdfs) # copying to Orc/Parquet/Hive from local src
    {
        # upload the data to HDFS
        df <- copy_to_hdfs(df, host=hdfs_host(dest))
        on.exit(delete_xdf(df))
    }

    dots <- exprs(...)
    arglst <- modify(list(df, outFile=dest), splice(dots))
    if(destInHdfs)
        callRx("rxDataStep", arglst)
    else local_exec(callRx("rxDataStep", arglst))
}


#' @rdname copy_to
#' @export
copy_to.RxHdfsFileSystem <- function(dest, df, name=NULL, ...)
{
    detectHdfsConnection() # fail early if no HDFS found

    if(is.character(df))
        df <- RxXdfData(df, fileSystem=RxNativeFileSystem())

    if(in_hdfs(df))
        stop("source is already in HDFS")

    host <- dest$hostName
    if(is.null(name))
        name <- getHdfsUserDir()

    # assume if name refers to a dir, we want to put it inside that dir
    if(hdfs_dir_exists(name, host=host))
    {
        path <- name
        name <- if(inherits(df, "RxFileData"))
            basename(df@file)
        else if(inherits(df, c("RxOdbcData", "RxTeradata")) && !is.null(df@table))
            df@table
        else basename(URLencode(deparse(substitute(df)), reserved=TRUE))
    }
    else
    {
        path <- dirname(name)
        name <- basename(name)
    }

    # if path is not specific, and host points to ADLS storage, save file there instead of native HDFS
    path <- makeHdfsUri(host, normalizeHdfsPath(path))

    if(!is_composite_xdf(df))
    {
        # create a composite copy of non-composite src
        # this happens on client if remote
        message("Creating composite Xdf from non-composite data source")

        localName <- tbl_xdf(fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE)@file
        df <- local_exec(as_composite_xdf(df, localName, overwrite=TRUE))
        name <- validateXdfFile(name, TRUE)
        on.exit(delete_xdf(df))
    }

    hdfs_upload(df@file, path, overwrite=TRUE, host=host, ...)
    hdfsFile <- normalizeHdfsPath(file.path(path, basename(df@file), fsep="/"))
    xdf <- RxXdfData(hdfsFile, fileSystem=dest, createCompositeSet=TRUE)

    if(basename(xdf@file) != name)
        rename_xdf(xdf, name)
    else xdf
}


#' @rdname copy_to
#' @export
copy_to_hdfs <- function(..., host=hdfs_host(), port=rxGetOption("hdfsPort"))
{
    copy_to(RxHdfsFileSystem(hostName=host, port=port), ...)
}


