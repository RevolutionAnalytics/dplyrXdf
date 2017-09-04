#' The data frame method simply calls \code{factor} to convert the specified columns into factors.
#'
#' @examples
#'
#' # factorise() also works with data frames
#' tbl4 <- factorise(mtcars, cyl)
#' tbl_types(tbl4)
#' @rdname factorise
#' @export
factorise.data.frame <- function(.data, ...)
{
    # add a method for data frames because merging with xdfs can get messy
    dots <- rlang::quos(..., .named = TRUE)
    if(".rxArgs" %in% names(dots))
    {
        warning("factorise() with data frame input doesn't use .rxArgs argument", call.=FALSE)
        dots$.rxArgs <- NULL
    }
    if(".outFile" %in% names(dots))
    {
        warning("factorise() with data frame input only outputs data frames", call.=FALSE)
        dots$.outFile <- NULL
    }

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
