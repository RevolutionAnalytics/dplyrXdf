#' @export
select_.RxFileData <- function(.data, ..., .output, .rxArgs, .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # .output and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.output)) .output <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs

    grps <- groups(.data)
    vars <- c(grps, select_vars_(names(.data), exprs))

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    .output <- createOutput(.data, .output)

    # need to use rxImport on non-Xdf data sources because of bugs in rxDataStep
    cl <- if(inherits(.data, "RxXdfData"))
        substitute(rxDataStep(.data, .output, varsToKeep=.expr, overwrite=TRUE),
            list(.expr=vars))
    else substitute(rxImport(.data, .output, varsToKeep=.expr, overwrite=TRUE),
            list(.expr=vars))
    cl[names(.rxArgs)] <- .rxArgs

    .data <- eval(cl)
    simpleRegroup(.data, grps)
}

