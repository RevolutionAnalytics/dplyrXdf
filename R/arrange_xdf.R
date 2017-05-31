#' Arrange the rows in an Xdf file
#'
#' @param data An Xdf data source, tbl, or other RevoScaleR file data source.
#' @param ... List of unquoted variable names. Use \code{desc} to sort in descending order.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
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
#' @aliases arrange
#' @export
arrange.RxXdfData <- function(.data, ..., .by_group=FALSE, .outFile, .rxArgs)
{
    stopIfHdfs(.data, "arrange not supported on HDFS")

    dots <- rlang::quos(...)

    transforms <- lapply(dots, get_expr)
    # turn a list of quoted expressions into a quoted list of expressions
    transforms <- if(length(transforms) > 0)
        as.call(c(as.name("list"), transforms))
    else NULL

    if(any(sapply(transforms, is.null)))
        stop("do not set variables to NULL in transmute; to delete variables, leave them out of the function call")

    grps <- group_vars(.data)
    if(.by_group)
    {
        dots <- c(grps, dots)
    }

    exprs <- lapply(dots, function(e) rlang::get_expr(e))
    desc <- sapply(exprs, function(e) !is.name(e) && identical(e[[1]], as.name("desc")))
    vars <- sapply(exprs, function(e) all.vars(e)[1])

    arglst <- list(.data, sortByVars=vars, decreasing=desc)
    arglst <- doExtraArgs(arglst, .data, rlang::enexpr(.rxArgs), .outFile)

    on.exit(deleteIfTbl(.data))
    rlang::invoke("rxSort", arglst, .env=parent.frame())
    simpleRegroup(.data, grps)
}


#' @rdname arrange
#' @export
arrange.rxFileData <- function(.data, ...)
{
    # rxSort only works with xdf files, not other data sources
    .data <- as(.data, "tbl_xdf")
    arrange.RxXdfData(.data, ...)
}

