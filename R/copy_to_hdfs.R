#' Upload a dataset to HDFS
#'
#' @param dest The destination source: an object of class \code{\link{RxHdfsFileSystem}}.
#' @param df A dataset: can be a filename, an Xdf data source object, another RevoScaleR data source, or anything that can be coerced to a data frame.
#' @param name The filename, optionally including the path, for the uploaded Xdf file. Defaults to the name of the source dataset, in the user's HDFS home directory.
#' @param ... Further arguments to \code{\link{rxHadoopCommand}}.
#'
#' @details
#' This is the RevoScaleR HDFS method for the dplyr \code{\link[dplyr]{copy_to}} function, for uploading data to a remote database/src. The method should work with any RevoScaleR data source, or with any R object that can be converted into a data frame. If the data is not already in Xdf format, it is first imported into Xdf, and then uploaded.
#'
#' The code will handle both the cases where you are logged into the edge node of a Hadoop/Spark cluster, and if you are a remote client. For the latter case, the uploading is a two-stage process: the data is first transferred to the native filesystem of the edge node, and then copied from the edge node into HDFS.
#'
#' @section Note on composite Xdf
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

    if(inherits(df, "RxXdfData"))
    {
        if(in_hdfs(df))
            stop("source is already in HDFS")
    }
    else # if not an Xdf, create an Xdf
    {
        localName <- file.path(get_dplyrxdf_dir("native"), if(is.null(name))
            URLencode(deparse(substitute(df)), reserved=TRUE)
        else basename(name))
        on.exit(delete_xdf(df))
        df <- local_exec(as_composite_xdf(df, localName))
    }

    if(!is_composite_xdf(df))
    {
        # create a composite copy of non-composite src
        # this happens on client if remote
        message("Creating composite copy of non-composite Xdf ", df@file)

        localName <- file.path(get_dplyrxdf_dir("native"), basename(df@file))
        on.exit(delete_xdf(df))
        df <- local_exec(as_composite_xdf(df, localName))
    }

    if(is.null(name))
        name <- getHdfsUserDir()

    nameIsPath <- hdfs_dir_exists(name)
    if(nameIsPath)
    {
        path <- name
        name <- basename(df@file)
    }
    else
    {
        path <- dirname(name)
        name <- basename(name)
    }

    hdfs_upload(df@file, path, overwrite=TRUE, ...)

    xdf <- RxXdfData(file.path(path, basename(df@file), fsep="/"), fileSystem=dest, createCompositeSet=TRUE)
    if(basename(xdf@file) != name)
        rename_xdf(xdf, name)
    else xdf
}


#' @details
#' The \code{copy_to_hdfs} function is a simple wrapper that avoids having to create an explicit filesystem object.
#' @rdname copy_to
#' @export
copy_to_hdfs <- function(...)
{
    copy_to(RxHdfsFileSystem(), ...)
}


