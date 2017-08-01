#' @export
as_composite_xdf <- function(.data, file=NULL, ...)
{
    as_xdf(.data, file=file, composite=TRUE, ...)
}


#' @export
as_standard_xdf <- function(.data, file=NULL, ...)
{
    as_xdf(.data, file=file, composite=FALSE, ...)
}


#' @export
as_xdf <- function(.data, ...)
{
    UseMethod("as_xdf")
}


#' @export
as_xdf.RxXdfData <- function(.data, file=NULL, composite=in_hdfs(.data), ...)
{
    compositeIn <- isCompositeXdf(.data)
    if(compositeIn == composite && is.null(file))
        return(.data)

    if(is.null(file))
        file <- .data@file
    file <- validateXdfFile(file, composite)

    if(compositeIn == composite)
        return(copy_xdf(.data, file))

    out <- modifyXdf(.data, file=file, createCompositeSet=composite)
    rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, ...)
}


#' @export
as_xdf.RxFileData <- function(.data, file=NULL, composite=in_hdfs(.data), ...)
{
    if(is.null(file))
        file <- .data@file
    file <- validateXdfFile(file, composite)

    out <- RxXdfData(file=file, fileSystem=rxGetFileSystem(.data), createCompositeSet=composite)
    rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, ...)
}


#' @export
as_xdf.RxDataSource <- function(.data, file=NULL, composite=NULL, ...)
{
    hdfsDetected <- !is.na(isRemoteHdfsClient(FALSE)) || in_hdfs()
    if(is.null(composite))
        composite <- hdfsDetected

    if(is.null(file))
        file <- basename(tempfile())
    file <- validateXdfFile(file, composite)

    out <- RxXdfData(file=file, fileSystem=rxGetFileSystem(.data), createCompositeSet=composite)
    rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, ...)
}


#' @export
as_xdf.default <- function(.data, file=NULL, composite=NULL, ...)
{
    hdfsDetected <- !is.na(isRemoteHdfsClient(FALSE)) || in_hdfs()
    if(is.null(composite))
        composite <- hdfsDetected

    .data <- as.data.frame(.data)
    if(is.null(file))
        file <- basename(tempfile())
    file <- validateXdfFile(file, composite)

    out <- RxXdfData(file=file, fileSystem=rxGetFileSystem(), createCompositeSet=composite)
    rxDataStep(.data, out, rowsPerRead=.dxOptions$rowsPerRead, ...)
}
