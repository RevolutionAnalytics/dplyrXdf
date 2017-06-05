#' @include factorise_xdf.R
NULL

#' The data frame method simply calls \code{factor} to convert the specified columns into factors.
#' @rdname factorise
#' @export
factorise.data.frame <- function(.data, ...)
{
    # add a method for data frames because merging with xdfs can get messy
    dots <- rlang::quos(..., .named = TRUE)

    grps <- group_vars(.data)
    types <- varTypes(.data)
    vars <- factoriseVars(types, dots)

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
