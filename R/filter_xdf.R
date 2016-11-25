#' Subset a data source by rows
#'
#' @param .data A data source.
#' @param ... Expressions to filter by.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#' @return
#' An object representing the filtered data. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[dplyr]{filter}} in package dplyr
#' @rdname filter
#' @aliases filter filter_
#' @export
filter_.RxFileData <- function(.data, ..., .outFile, .rxArgs, .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.outFile)) .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs
    grps <- groups(.data)

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    all_exprs <- exprs[[1]]
    if(length(exprs) > 1) for(i in 2:length(exprs))  # loop taken from dplyr:::and_expr
    {
        all_exprs <- substitute(left & right, list(left=all_exprs, right=exprs[[i]]))
    }

    .outFile <- createOutput(.data, .outFile)
    cl <- substitute(rxDataStep(.data, .outFile, rowSelection=.expr, overwrite=TRUE),
        list(.expr=all_exprs))
    cl[names(.rxArgs)] <- .rxArgs

    .data <- eval(cl)
    simpleRegroup(.data, grps)
}
