#' Detect and coerce to Xdf data source objects
#'
#' Functions to detect and coerce to Xdf data source objects.
#'
#' @param .data An R object that can be coerced to an Xdf data source. This includes another existing Xdf data source; see details below.
#' @param file The name for the Xdf data file, optionally with path. If not supplied, this is taken from \code{.data}.
#' @param composite Whether to create a composite Xdf file. Defaults to TRUE if \code{.data} is stored in HDFS, and FALSE otherwise.
#' @param overwrite Whether to overwrite any existing file.
#' @param ... Other (named) arguments to pass to \code{\link{rxDataStep}}.
#'
#' @details
#' The \code{as_xdf} function takes the object given by \code{.data} and imports its data into an Xdf file, returning a data source pointing to that file. The file can be either a standard or a \emph{composite} Xdf, as given by the \code{composite} argument. A composite Xdf is actually a directory containing data and metadata files; it can be manipulated by the RevoScaleR functions as if it were a single dataset.
#'
#' The \code{as_standard_xdf} and \code{as_composite_xdf} functions are shorthand for \code{as_xdf(*, composite=FALSE)} and \code{as_xdf(*, composite=TRUE)} respectively; they always create either a standard or composite Xdf. You can use this to switch an existing Xdf data source from one type of Xdf to the other. Note that Xdf files in HDFS must always be composite.
#'
#' Passing a \code{tbl_xdf} object to an \code{as} function will strip off the tbl information, returning a raw Xdf data source. This can be useful for resetting the beginning of a pipeline.
#'
#' The \code{file} argument gives the name of the Xdf file to create. If not specified, this is taken from the input data source where possible (for Xdf and file data sources, including text). Otherwise, it is taken from the name of the input R object. If no directory is specified, the file is created in the current working directory (if in the native filesystem) or in the user's home directory (in HDFS).
#'
#' You can use the \code{as} functions with any RevoScaleR data source, or otherwise with any R object that can be turned into a data frame. The resulting Xdf file will be created in the same filesystem as the input data source. If the input does not have a filesystem, for example if it is an in-database table or a data frame, the file is created in the native filesystem.
#'
#' @return
#' For the \code{as} functions, an Xdf data source object pointing to the created file. For the \code{is} functions, a logical value.
#'
#' @seealso
#' \code{\link{as}}, \code{\link{is}}, \code{\link{inherits}},
#' \code{\link{persist}}, \code{\link{rxDataStep}}, \code{\link{rxImport}}
#'
#' @examples
#' # convert data frame to Xdf
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#' is_xdf(mtcars)
#' is_xdf(mtx)
#' is_composite_xdf(mtx)
#'
#' # as_xdf() on an Xdf file without any args is a no-op
#' as_xdf(mtx)
#'
#' ## some common uses for as_xdf:
#'
#' # convert a standard Xdf file to composite
#' mtc <- as_composite_xdf(mtx, overwrite=TRUE)
#' is_composite_xdf(mtc)
#'
#' # convert a tbl_xdf to Xdf (could also use persist())
#' tbl <- mtx %>% mutate(mpg2=2 * mpg)
#' as_xdf(tbl, file="mtcars_mutate.xdf", overwrite=TRUE)
#'
#' # import selected columns of a text file to Xdf
#' write.csv(mtcars, "mtcars.csv", row.names=FALSE)
#' mtt <- RxTextData("mtcars.csv")
#' mtx <- as_xdf(mtt, overwrite=TRUE, varsToKeep=c("mpg", "cyl"))
#'
#' # import a database table to Xdf
#' \dontrun{
#' table_db <- RxSqlServerData(table="mytable", server="sqlserver", databaseName="dbname", ...)
#' table_xdf <- as_xdf(table_db)
#' }
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
as_xdf.RxXdfData <- function(.data, file=NULL, composite=is_composite_xdf(.data), overwrite=FALSE, ...)
{
    compositeIn <- is_composite_xdf(.data)

    if(in_hdfs(.data) && !composite)
        stop("only composite Xdf files supported in HDFS")

    if(is.null(file))
        file <- .data@file
    file <- validateXdfFile(file, composite)

    if(compositeIn == composite)
    {
        if(file == .data@file)
            return(as(.data, "RxXdfData"))

        asXdfOverwriteCheck(file, overwrite, in_hdfs(.data), hdfs_host(.data))
        return(copy_xdf(.data, file))
    }

    asXdfOverwriteCheck(file, overwrite, in_hdfs(.data), hdfs_host(.data))

    out <- modifyXdf(.data, file=file, createCompositeSet=composite)
    arglst <- asXdfArgList(.data, outFile=out, rowsPerRead=.dxOptions$rowsPerRead, overwrite=overwrite, ...)
    callRx("rxDataStep", arglst)
}


#' @rdname as_xdf
#' @export
as_xdf.RxDataSource <- function(.data, file=NULL, composite=in_hdfs(.data), overwrite=FALSE, ...)
{
    if(in_hdfs(.data) && !composite)
        stop("only composite Xdf files supported in HDFS")

    if(is.null(file))
    {
        file <- if(inherits(.data, c("RxFileData", "RxOrcData", "RxParquetData")))
            .data@file
        else if(inherits(.data, c("RxOdbcData", "RxTeradata", "RxHiveData")) && !is.null(.data@table))
            .data@table
        else URLencode(deparse(substitute(.data)), reserved=TRUE)
        file <- basename(file)
    }

    file <- validateXdfFile(file, composite)
    asXdfOverwriteCheck(file, overwrite, in_hdfs(.data), hdfs_host(.data))

    # RxHiveData is considered to be in native filesystem (!)
    fs <- if(inherits(.data, "RxHiveData"))
        RxHdfsFileSystem(hostName=hdfs_host())
    else rxGetFileSystem(.data)
        
    out <- RxXdfData(file=file, fileSystem=fs, createCompositeSet=composite)
    arglst <- asXdfArgList(.data, outFile=out, rowsPerRead=.dxOptions$rowsPerRead, overwrite=overwrite, ...)

    if(in_hdfs(out))
        callRx("rxDataStep", arglst)
    else local_exec(callRx("rxDataStep", arglst))
}


#' @rdname as_xdf
#' @export
as_xdf.default <- function(.data, file=NULL, composite=FALSE, overwrite=FALSE, ...)
{
    if(is.null(file))
        file <- URLencode(deparse(substitute(.data)), reserved=TRUE)
    file <- validateXdfFile(file, composite)
    asXdfOverwriteCheck(file, overwrite, FALSE)

    .data <- as.data.frame(.data)

    out <- RxXdfData(file=file, fileSystem=RxNativeFileSystem(), createCompositeSet=composite)
    arglst <- asXdfArgList(.data, outFile=out, rowsPerRead=.dxOptions$rowsPerRead, overwrite=overwrite, ...)
    local_exec(callRx("rxDataStep", arglst))
}


# handle overwriting semantics properly -- always treat 'file' as filename, not dirname
asXdfOverwriteCheck <- function(file, overwrite, inHdfs, hdfsHost=NULL)
{
    err <- FALSE

    if(inHdfs)
    {
        if(hdfs_file_exists(file, host=hdfsHost))
        {
            if(overwrite)
                hdfs_dir_remove(file, host=hdfsHost)
            else err <- TRUE
        }
    }
    else
    {
        if(file.exists(file))
        {
            if(overwrite)
                unlink(file, recursive=TRUE)
            else err <- TRUE
        }
    }

    if(err)
        stop("destination file '", file, "' exists; set overwrite=TRUE to replace it", call.=FALSE)
}


# rowSelection and transforms args require special treatment because of NSE
asXdfArgList <- function(.data, outFile, ...)
{
    dots <- quos(..., .named=TRUE)
    dots <- sapply(names(dots), function(n)
    {
        if(n %in% c("rowSelection", "transforms"))
            get_expr(dots[[n]])
        else eval_tidy(dots[[n]])
    }, simplify=FALSE)

    init <- list(.data, outFile=outFile, rowsPerRead=.dxOptions$rowsPerRead)
    modify(init, splice(dots))
}

