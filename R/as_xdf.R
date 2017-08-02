#' @export
as_composite_xdf <- function(.data, ...)
{
    as_xdf(.data, ..., composite=TRUE)
}


#' @export
as_standard_xdf <- function(.data, ...)
{
    as_xdf(.data, ..., composite=FALSE)
}


#' @export
as_xdf <- function(.data, ...)
{
    UseMethod("as_xdf")
}


#' @export
as_xdf.RxXdfData <- function(.data, file=NULL, composite=NULL, overwrite=TRUE, ...)
{
    if(in_hdfs(.data) && !composite)
        stop("only composite Xdf files supported in HDFS")

    compositeIn <- is_composite_xdf(.data)
    if(is.null(composite))
        composite <- compositeIn

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
    rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE, ...)
}


#' @export
as_xdf.RxFileData <- function(.data, file=NULL, composite=in_hdfs(.data), ...)
{
    if(in_hdfs(.data) && !composite)
        stop("only composite Xdf files supported in HDFS")

    if(is.null(file))
        file <- .data@file
    file <- validateXdfFile(file, composite)

    out <- RxXdfData(file=file, fileSystem=rxGetFileSystem(.data), createCompositeSet=composite)
    rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE, ...)
}


#' @export
as_xdf.RxDataSource <- function(.data, file=NULL, composite=NULL, ...)
{
    hdfsDetected <- !is.na(isRemoteHdfsClient(FALSE)) || in_hdfs()
    if(is.null(composite))
        composite <- hdfsDetected

    if(in_hdfs(.data) && !composite)
        stop("only composite Xdf files supported in HDFS")

    if(is.null(file))
        file <- basename(tempfile())
    file <- validateXdfFile(file, composite)

    out <- RxXdfData(file=file, fileSystem=rxGetFileSystem(.data), createCompositeSet=composite)
    rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE, ...)
}


#' @export
as_xdf.default <- function(.data, file=NULL, composite=NULL, ...)
{
    hdfsDetected <- !is.na(isRemoteHdfsClient(FALSE)) || in_hdfs()
    if(is.null(composite))
        composite <- hdfsDetected

    if(is.null(file))
        file <- basename(tempfile())
    file <- validateXdfFile(file, composite)

    .data <- as.data.frame(.data)

    out <- RxXdfData(file=file, fileSystem=RxNativeFileSystem(), createCompositeSet=composite)
    local_exec(rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE, ...))
}
