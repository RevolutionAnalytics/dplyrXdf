#' @export
filter_.RxFileData <- function(.data, ..., .output, .rxArgs, .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # .output and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.output)) .output <- dots$output
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

    .output <- createOutput(.data, .output)
    cl <- substitute(rxDataStep(.data, .output, rowSelection=.expr, overwrite=TRUE),
        list(.expr=all_exprs))
    cl[names(.rxArgs)] <- .rxArgs

    .data <- eval(cl)
    simpleRegroup(.data, grps)
}
