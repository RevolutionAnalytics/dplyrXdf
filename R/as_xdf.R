#' Detect and coerce to Xdf data source objects
#'
#' Functions to detect and coerce to Xdf data source objects.
#'
#' @param .data An R object that can be coerced to an Xdf data source. This includes another existing Xdf data source; see details below.
#' @param file The path/filename for the Xdf data file. If not supplied, this is taken from \code{.data}.
#' @param composite Whether to create a composite Xdf. Defaults to TRUE if \code{.data} is stored in HDFS, or if a HDFS connection is detected; FALSE otherwise.
#' @param overwrite Whether to overwrite any existing file.
#' @param ... Other arguments to pass to \code{\link{rxDataStep}}.
#'
#' @details
#' The \code{as_xdf} function takes the object given by \code{.data} and imports its data into an Xdf file, returning a data source pointing to that file. The file can be either a standard or a \emph{composite} Xdf, as given by the \code{composite} argument. A composite Xdf is actually a directory containing data and metadata files; it can be manipulated by the RevoScaleR functions as if it were a single dataset.
#'
#' The \code{as_standard_xdf} and \code{as_composite_xdf} functions are shorthand for \code{as_xdf(*, composite=FALSE)} and \code{as_xdf(*, composite=TRUE)} respectively; they always create either a standard or composite Xdf. You can use this to switch an existing Xdf data source from one type of Xdf to the other. Note that Xdf files in HDFS must always be composite.
#'
#' Passing a \code{tbl_xdf} object to an \code{as} function will strip off the tbl information, returning a raw Xdf data source. This can be useful for resetting the beginning of a pipeline.
#'
#' The \code{file} argument gives the name of the Xdf file to create. If not specified, this is taken from the input data source where possible (for Xdf and file data sources, including text). Otherwise, it is taken from the name of R input object. If no directory is specified, the file is created in the current working directory (if in the native filesystem) or in the user directory (in HDFS).
#'
#' You can use the \code{as} functions with any RevoScaleR data source, or otherwise with any R object that can be turned into a data frame. The resulting Xdf file will be created in the same filesystem as the input data source. If the input does not have a filesystem, for example if it is an in-database table or a data frame, the file is created in the native filesystem.
#'
#' @return
#' For the \code{as} functions, an Xdf data source object pointing to the created file. For the \code{is} functions, a TRUE/FALSE value.
#'
#' @seealso
#' \code{\link{as}}, \code{\link{is}}, \code{\link{inherits}},
#' \code{\link{rxDataStep}}, \code{\link{rxImport}}
#'
#' @rdname as_xdf
#' @export
as_composite_xdf <- function(...)
{
    as_xdf(..., composite=TRUE)
}


#' @rdname as_xdf
#' @export
as_standard_xdf <- function(...)
{
    as_xdf(..., composite=FALSE)
}


#' @rdname as_xdf
#' @export
as_xdf <- function(.data, ...)
{
    UseMethod("as_xdf")
}


#' @rdname as_xdf
#' @export
as_xdf.RxXdfData <- function(.data, file=NULL, composite=NULL, overwrite=TRUE, ...)
{
    compositeIn <- is_composite_xdf(.data)
    if(is.null(composite))
        composite <- compositeIn

    if(in_hdfs(.data) && !composite)
        stop("only composite Xdf files supported in HDFS")

    if(compositeIn == composite)
    {
        if(is.null(file))
            return(as(.data, "RxXdfData"))
        else return(copy_xdf(.data, file))
    }

    if(is.null(file))
        file <- .data@file
    file <- validateXdfFile(file, composite)

    if(compositeIn == composite)
        return(copy_xdf(.data, file))

    out <- modifyXdf(.data, file=file, createCompositeSet=composite)
    rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, overwrite=overwrite, ...)
}


#' @rdname as_xdf
#' @export
as_xdf.RxFileData <- function(.data, file=NULL, composite=in_hdfs(.data), overwrite=TRUE, ...)
{
    if(in_hdfs(.data) && !composite)
        stop("only composite Xdf files supported in HDFS")

    if(is.null(file))
        file <- .data@file
    file <- validateXdfFile(file, composite)

    out <- RxXdfData(file=file, fileSystem=rxGetFileSystem(.data), createCompositeSet=composite)
    rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, overwrite=overwrite, ...)
}


#' @rdname as_xdf
#' @export
as_xdf.RxDataSource <- function(.data, file=NULL, composite=NULL, overwrite=TRUE, ...)
{
    hdfsDetected <- !is.na(detectHdfsConnection(FALSE)) || in_hdfs(.data)
    if(is.null(composite))
        composite <- hdfsDetected

    if(in_hdfs(.data) && !composite)
        stop("only composite Xdf files supported in HDFS")

    if(is.null(file))
        file <- URLencode(deparse(substitute(.data)), reserved=TRUE)
    file <- validateXdfFile(file, composite)

    out <- RxXdfData(file=file, fileSystem=rxGetFileSystem(.data), createCompositeSet=composite)
    if(in_hdfs(out))
        rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, overwrite=overwrite, ...)
    else local_exec(rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, overwrite=overwrite, ...))
}


#' @rdname as_xdf
#' @export
as_xdf.default <- function(.data, file=NULL, composite=NULL, overwrite=TRUE, ...)
{
    hdfsDetected <- !is.na(detectHdfsConnection(FALSE)) || in_hdfs()
    if(is.null(composite))
        composite <- hdfsDetected

    if(is.null(file))
        file <- URLencode(deparse(substitute(.data)), reserved=TRUE)
    file <- validateXdfFile(file, composite)

    .data <- as.data.frame(.data)

    out <- RxXdfData(file=file, fileSystem=RxNativeFileSystem(), createCompositeSet=composite)
    local_exec(rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, overwrite=overwrite, ...))
}
