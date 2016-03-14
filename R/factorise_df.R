#' @include factorise_xdf.R
NULL

#' The data frame method simply calls \code{factor} to convert the specified columns into factors.
#' @rdname factorise
#' @export
factorise_.data.frame <- function(.data, ..., .dots)
{
    # add a method for data frames because merging with xdfs can get messy
    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    grps <- groups(.data)
    types <- sapply(.data, function(x) class(x)[1])
    vars <- factorise_vars(types, exprs)

    .data[] <- lapply(names(.data), function(n) {
        x <- .data[[n]]
        if(n %in% vars$blankArgs)
            factor(x)
        else if(n %in% names(vars$newlevelArgs))
            factor(x, levels=as.character(vars$newlevelArgs[[n]]))
        else x
    })
    .data
}
