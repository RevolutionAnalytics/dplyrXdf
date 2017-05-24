#' Arrange the rows in an Xdf file
#'
#' @param data An Xdf data source, tbl, or other RevoScaleR file data source.
#' @param ... List of unquoted variable names. Use \code{desc} to sort in descending order.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .dots Used to work around non-standard evaluation. See the dplyr documentation for details.
#'
#' @details
#' The underlying RevoScaleR function is \code{rxSort}. This has many sorting options, including removing duplicated keys, adding a column of frequency counts, and so on.
#'
#' @return
#' An object representing the sorted data. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link{rxSort}}, \code{\link[dplyr]{arrange}} in package dplyr
#' @rdname arrange
#' @aliases arrange arrange_
#' @export
arrange_.RxFileData <- function(.data, ..., .outFile, .rxArgs, .dots)
{
    stopIfHdfs(.data, "arrange not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.outFile)) .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs
    grps <- group_vars(.data)

    # rxSort only works with xdf files, not other data sources
    if(!inherits(.data, "RxXdfData"))
        .data <- tbl(.data)

    exprs <- c(lapply(grps, as.name), exprs)
    desc <- sapply(exprs, function(e) !is.name(e) && identical(e[[1]], as.name("desc")))
    vars <- sapply(exprs, function(e) all.vars(e)[1])

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    .outFile <- createOutput(.data, .outFile)
    cl <- quote(rxSort(.data, .outFile, sortByVars=vars, decreasing=desc, overwrite=TRUE))
    cl[names(.rxArgs)] <- .rxArgs

    .data <- eval(cl)
    simpleRegroup(.data, grps)
}



