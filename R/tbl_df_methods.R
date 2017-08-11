#' @include tbl_xdf.R
NULL

#' Convert a data source or tbl to a data frame
#'
#' @param x A data source object, or tbl wrapping the same.
#' @param maxRowsByCols the maximum dataset size to convert, expressed in terms of rows times columns. Defaults to NULL, meaning no maximum.
#' @param row.names,optional For compatibility with the \code{as.data.frame} generic. Not used.
#' @param ... Other arguments to \code{rxDataStep}.
#' @details
#' These are simple wrappers around \code{\link[RevoScaleR]{rxDataStep}}, with the check on the maximum table size turned off. You should ensure that you have enough memory for your data.
#'
#' \code{as.data.frame} converts a data source object (typically an Xdf file, but can also be any data source type that \code{rxDataStep} supports) into a data frame. The \code{$} and \code{[[} methods extract a single column from a data source, as a vector.
#'
#' @seealso
#' \code{\link[base]{as.data.frame}}, \code{\link[dplyr]{collect}}
#' @aliases as.data.frame
#' @rdname as.data.frame
#' @export
as.data.frame.RxFileData <- function(x, maxRowsByCols=NULL, row.names=NULL, optional=TRUE, ...)
{
    # calling rxDataStep on HDFS data from remote client is bog-slow, use direct download instead
    if(in_hdfs(x))
        collect(x, as_data_frame=TRUE, maxRowsByCols=maxRowsByCols, ...)
    else local_exec(rxDataStep(x, outFile=NULL, maxRowsByCols=maxRowsByCols, ...))
}


#' Download a dataset to the local machine
#'
#' @param x An Xdf data source object.
#' @param as_data_frame Should the downloaded data be converted to a data frame, or left as an Xdf file?
#' @param ... If the output is to be a data frame, further arguments to the \code{as.data.frame} method.
#'
#' @details
#' The \code{collect} and \code{compute} functions can be used for two purposes: to download a dataset stored in HDFS to the native filesystem; or to convert a dataset (whether stored in HDFS or not) to a data frame. If \code{x} is an Xdf data source in HDFS, the data is downloaded as a tbl_xdf in the dplyrXdf working directory.
#'
#' The functions differ only in the default value of the \code{as_data_frame} argument. By default \code{collect} will always output a data frame, while \code{compute} will only do so if the source data was \emph{not} downloaded from HDFS. Note that if \code{as_data_frame} is FALSE and the source data is on the native filesystem, then \code{collect}/\code{compute} is effectively a no-op.
#'
#' The code will handle both the cases where you are logged into the edge node of a Hadoop/Spark cluster, and if you are a remote client. For the latter case, the downloading is a two-stage process: the data is first transferred from HDFS to the native filesystem of the edge node, and then downloaded from the edge node to the client.
#'
#' If you want to look at the first few rows of an Xdf file, it may be faster to use \code{compute}) to copy the entire file off HDFS, and then run \code{head}, than to run \code{head} on the original. This is due to quirks in how RevoScaleR works in Spark and Hadoop.
#'
#' @return
#' If \code{as_data_frame} is FALSE, a data frame. Otherwise, a tbl_xdf data source.
#'
#' @seealso
#' \code{\link[dplyr]{compute}} in package dplyr, \code{\link{copy_to}} for uploading to HDFS
#'
#' @aliases collect, compute
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
        hdfs_download(x@file, localXdf@file, overwrite=TRUE)
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
        hdfs_download(x@file, localXdf@file, overwrite=TRUE)
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


#' @param name The name of a column to extract from a data source object
#' @rdname as.data.frame
#' @export
"$.RxFileData" <- function(x, name)
{
    rxDataStep(unTbl(x), outFile=NULL, varsToKeep=name, maxRowsByCols=NULL)[[1]]
}


## rxGetVarInfo depends on Rx* sources not having a [ method
##' @export
#"[.RxFileData" <- function(x, i, j, drop=FALSE, maxRowsByCols=NULL, ...)
#{
    ## try to detect if list-style indexing used (no j argument, missing or otherwise)
    #missingI <- missing(i)
    #missingJ <- missing(j)
    #nArgs <- nargs()
    #nOtherArgs <- length(match.call(expand=FALSE)$`...`) + !missing(drop) + !missing(maxRowsByCols)
    #asList <- (nArgs - nOtherArgs <= 2) || (nArgs - nOtherArgs <= 1 && missingJ)

    #if(asList)
    #{
        ## morph to matrix-style indexing: set 2nd index (if present) to 1st, set 1st index to missing
        #if(!missingI)
            #j <- i
        #missingJ <- missingI
        #missingI <- TRUE
    #}

    ## from here on, assume matrix-style indexing (2 indices)
    #varsToKeep <- if(!missingJ)
    #{
        #nams <- names(x)
        #if(is.logical(j) || is.numeric(j))
            #varsToKeep <- nams[j]
        #else varsToKeep <- as.character(j)
    #}
    #else NULL
    #df <- rxDataStep(x, outFile=NULL, varsToKeep=varsToKeep, maxRowsByCols=maxRowsByCols, ...)
    ## must do this separately because by semantics of [, row selection expression is evaluated in calling frame
    #if(!missingI)
        #df[i, , drop=drop]
    #else df
#}


#' @rdname as.data.frame
#' @export
"[[.RxFileData" <- function(x, name, maxRowsByCols=NULL, ...)
{
    nams <- names(x)
    if(length(name) > 1)
        stop("attempt to select more than one column")
    varsToKeep <- if(is.logical(name) || is.numeric(name))
        nams[name]
    else as.character(name)

    rxDataStep(unTbl(x), outFile=NULL, varsToKeep=varsToKeep, maxRowsByCols=maxRowsByCols, ...)[[1]]
}


#' @rdname as.data.frame
#' @export
pull.RxFileData <- function(.data, var=-1)
{
    # exactly the same as for data frames
    var <- select_var(names(.data), !(!enquo(var)))
    .data[[var]]
}
