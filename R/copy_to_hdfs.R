#' Upload a dataset to HDFS
#'
#' @param dest The destination source: an object of class \code{\link{RxHdfsFileSystem}}.
#' @param df A dataset: can be a filename, an Xdf data source object, another RevoScaleR data source, or anything that can be coerced to a data frame.
#' @param name The filename, optionally including the path, for the uploaded Xdf file. The default upload location for the host HDFS filesystem is the user's home directory, and for an attached Azure Data Lake Store, the root directory.
#' @param ... Further arguments to \code{\link{rxHadoopCommand}}.
#'
#' @details
#' This is the RevoScaleR HDFS method for the dplyr \code{\link[dplyr]{copy_to}} function, for uploading data to a remote database/src. The method should work with any RevoScaleR data source, or with any R object that can be converted into a data frame. If the data is not already in Xdf format, it is first imported into Xdf, and then uploaded.
#'
#' The function will handle both the cases where you are logged into the edge node of a Hadoop/Spark cluster, and if you are a remote client. For the latter case, the uploading is a two-stage process: the data is first transferred to the native filesystem of the edge node, and then copied from the edge node into HDFS.
#'
#' Similarly, it can handle uploading both to the host HDFS filesystem, and to an attached Azure Data Lake Store. If \code{dest} points to an ADLS host, the file will be uploaded to that filesystem, by default in the root directory. You can override this by supplying an explicit an explicit URI for the uploaded file, in the form \code{adl://azure.host.name/path}. The name for the host HDFS filesystem is \code{adl://host/}.
#'
#' @section Note on composite Xdf:
#' There are actually two kinds of Xdf files: standard and _composite_. A composite Xdf file is a directory containing multiple data and metadata files, which the RevoScaleR functions treat as a single dataset. Xdf files in HDFS must be composite in order to work properly; \code{copy_to} will convert an existing Xdf file into composite, if it's not already in that format. Non-Xdf datasets (data frames and other RevoScaleR data sources, such as text files) will similarly be uploaded as composite.
#'
#' @return
#' An Xdf data source object pointing to the uploaded data.
#'
#' @seealso
#' \code{\link{rxHadoopCopyFromClient}}, \code{\link{rxHadoopCopyFromLocal}},
#' \code{\link{collect}} and \code{\link{compute}} for downloading data from HDFS,
#' \code{\link{as_xdf}}, \code{\link{as_composite_xdf}}
#' @aliases copy_to
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
    {
        name <- if(host == "default")
            getHdfsUserDir()
        else "/"
    }

    # assume if name refers to a dir, we want to put it inside that dir
    if(hdfs_dir_exists(name))
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
    path <- makeHdfsUri(host, path)

    if(!is_composite_xdf(df))
    {
        # create a composite copy of non-composite src
        # this happens on client if remote
        message("Creating composite Xdf from non-composite data source")

        localName <- tbl_xdf(fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE)@file
        on.exit(delete_xdf(df))
        df <- local_exec(as_composite_xdf(df, localName, overwrite=TRUE))
    }

    hdfs_upload(df@file, path, overwrite=TRUE, ...)
    hdfsFile <- if(path == ".")
        basename(df@file)
    else file.path(path, basename(df@file), fsep="/")
    xdf <- RxXdfData(hdfsFile, fileSystem=dest, createCompositeSet=TRUE)

    if(basename(xdf@file) != name)
        rename_xdf(xdf, name)
    else xdf
}


#' @param host,port HDFS hostname and port number to connect to. Generally used only if you have an attached Azure Data Lake Store that you are accessing via HDFS.
#' @details
#' The \code{copy_to_hdfs} function is a simple wrapper for \code{copy_to} that avoids having to create an explicit filesystem object.
#' @rdname copy_to
#' @export
copy_to_hdfs <- function(..., host=rxGetOption("hdfsHost"), port=rxGetOption("hdfsPort"))
{
    copy_to(RxHdfsFileSystem(hostName=host, port=port), ...)
}


