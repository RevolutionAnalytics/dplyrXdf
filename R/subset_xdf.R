#' Subset a data source by rows and/or columns
#'
#' @param .data A data source object, or tbl wrapping the same.
#' @param subset Logical expression indicating rows to keep.
#' @param select Columns to select. See \code{link[dplyr]{select}} for the ways in which you can keep or drop columns.
#' @param ... Other arguments passed to lower-level functions.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#' @details
#' This is a method for the \code{\link[base]{subset}} generic from base R. It combines the effects of the \code{filter} and \code{select} verbs, allowing you to subset a RevoScaleR data source (typically an xdf file) by rows and columns simultaneously. The advantage of this for an Xdf file is that it significantly reduces the amount of I/O compared to doing the row and column subsetting in separate steps.
#'
#' @return
#' An object representing the subsetted data. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[base]{subset}}, \code{\link[dplyr]{filter}}, \code{\link[dplyr]{select}}, \code{\link[RevoScaleR]{rxDataStep}}
#' @aliases subset subset_
#' @rdname subset
#' @export
subset.RxFileData <- function(.data, subset=NULL, select=NULL, ...)
{
    subset <- lazyeval::lazy(subset)
    select <- lazyeval::lazy(select)
    subset_(.data, subset=subset, select=select, .dots = lazyeval::lazy_dots(...))
}


#' @rdname subset
#' @export
subset_ <- function(.data, ...)
{
    UseMethod("subset_")
}


#' @rdname subset
#' @export
subset_.RxFileData <- function(.data, subset=~NULL, select=~NULL, ..., .outFile, .rxArgs, .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.outFile)) .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs

    subset <- lazyeval::as.lazy(subset)$expr
    select <- lazyeval::as.lazy(select)$expr
    if(is.null(select))
        select <- names(.data)

    grps <- groups(.data)
    select <- c(grps, select_vars_(names(.data), select))

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    .outFile <- createOutput(.data, .outFile)

    # need to use rxImport on non-Xdf data sources because of bugs in rxDataStep
    cl <- if(inherits(.data, "RxXdfData"))
        substitute(rxDataStep(.data, .outFile, rowSelection=.rows, varsToKeep=.vars, overwrite=TRUE),
            list(.rows=subset, .vars=select))
    else substitute(rxImport(.data, .outFile, rowSelection=.rows, varsToKeep=.vars, overwrite=TRUE),
            list(.rows=subset, .vars=select))
    cl[names(.rxArgs)] <- .rxArgs

    .data <- eval(cl)
    simpleRegroup(.data)
}


