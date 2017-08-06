#' Upload a dataset to HDFS
#'
#' @param dest The destination source: an object of class \code{\link{RxHdfsFileSystem}}.
#' @param df A dataset: can be a filename, an Xdf data source object, another RevoScaleR data source, or anything that can be coerced to a data frame.
#' @param path The HDFS directory in which to store the uploaded dataset. Defaults to the user's HDFS home directory.
#' @param overwrite Whether to overwrite any existing file.
#' @param force_composite: Whether to force the uploaded dataset to be a composite Xdf file. See details below.
#' @param ... For \code{copy_to}, further arguments to \code{\link{rxHadoopCommand}}.
#'
#' @details
#' This is the RevoScaleR HDFS method for the dplyr \code{\link[dplyr]{copy_to}} function, for uploading data to a remote database/src. The method should work with any RevoScaleR data source, or with any R object that can be converted into a data frame. If the data is not already in Xdf format, it is first imported into Xdf, and then uploaded.
#'
#' The code will handle both the cases where you are logged into the edge node of a Hadoop/Spark cluster, and if you are a remote client. For the latter case, the uploading is a two-stage process: the data is first transferred to the native filesystem of the edge node, and then copied from the edge node into HDFS.
#'
#' The \code{force_composite} argument sets whether an existing Xdf file should be converted to composite format before uploading, if it is not already so. Xdf files stored in HDFS must be composite to work properly. Datasets that are not in Xdf format (data frames and other RevoScaleR data sources, such as text files) are always uploaded as composite.
#'
#' @return
#' An Xdf data source object pointing to the uploaded data.
#'
#' @seealso
#' \code{\link{rxHadoopCopyFromClient}}, \code{\link{rxHadoopCopyFromLocal}},
#' \code{\link{collect}} and \code{\link{compute}} for downloading data from HDFS
#' @aliases copy_to
#' @rdname copy_to
#' @export
copy_to.RxHdfsFileSystem <- function(dest, df, path=NULL, overwrite=FALSE, force_composite=TRUE, ...)
{
    detectHdfsConnection() # fail early if no HDFS found

    if(is.character(df))
        df <- RxXdfData(df)

    if(inherits(df, "RxXdfData"))
    {
        if(in_hdfs(df))
            stop("source is already in HDFS")
        localName <- df@file
    }
    else # if not an Xdf, create an Xdf
    {
        localName <- file.path(get_dplyrxdf_dir("native"), URLencode(deparse(substitute(df)), reserved=TRUE))
        df <- local_exec(as_composite_xdf(df, localName))
    }

    if(force_composite && !is_composite_xdf(df))
    {
        # create a composite copy of non-composite src
        # this happens on client if remote
        message("Creating composite copy of non-composite Xdf ", df@file)

        localName <- file.path(get_dplyrxdf_dir("native"), basename(df@file))
        df <- local_exec(as_composite_xdf(df, localName))
    }

    if(is.null(path))
        path <- getHdfsUserDir()

    hdfs_upload(df@file, path, overwrite=overwrite, ...)

    RxXdfData(file.path(path, basename(df@file), fsep="/"), fileSystem=dest, createCompositeSet=is_composite_xdf(df))
}


#' @details
#' The \code{copy_to_hdfs} function is a simple wrapper that avoids having to create an explicit filesystem object.
#' @rdname copy_to
#' @export
copy_to_hdfs <- function(...)
{
    copy_to(RxHdfsFileSystem(), ...)
}


