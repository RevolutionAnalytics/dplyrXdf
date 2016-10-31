#' Subset a data source by rows and/or columns
#'
#' @param .data A data source object, or tbl wrapping the same.
#' @param subset Logical expression indicating rows to keep.
#' @param select Columns to select. See \code{select} for the ways in which you can keep or drop columns.
#' @param ... Other arguments; use \code{\link{.rxArgs}} to pass arguments to \code{rxDataStep}.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#' @details
#' This is a method for the \code{\link[base]{subset}} generic from base R. It combines the effects of the \code{filter} and \code{select} verbs, allowing you to subset a RevoScaleR data source (typically an xdf file) by rows and columns simultaneously. The advantage of this for an xdf file is that it significantly reduces the amount of I/O compared to doing the row and column subsetting in separate steps.
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
subset_.RxFileData <- function(.data, subset=~NULL, select=~NULL, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # identify Revo-specific arguments
    dots <- dplyrXdf:::.rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    subset <- lazyeval::as.lazy(subset)$expr
    select <- lazyeval::as.lazy(select)$expr
    if(is.null(select))
        select <- names(.data)

    grps <- groups(.data)
    select <- c(grps, select_vars_(names(.data), select))

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    # need to use rxImport on non-Xdf data sources because of bugs in rxDataStep
    cl <- if(inherits(.data, "RxXdfData"))
        substitute(rxDataStep(.data, newTbl(.data), rowSelection=.rows, varsToKeep=.vars, overwrite=TRUE),
            list(.rows=subset, .vars=select))
    else substitute(rxImport(.data, newTbl(.data), rowSelection=.rows, varsToKeep=.vars, overwrite=TRUE),
            list(.rows=subset, .vars=select))
    cl[names(rxArgs)] <- rxArgs

    .data <- tbl(eval(cl), file=NULL, hasTblFile=TRUE)

    if(is.null(grps))
        .data
    else group_by_(.data, .dots=grps)
}


