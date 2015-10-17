#' @export
select_.RxFileData <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    grps <- groups(.data)
    vars <- c(grps, select_vars_(names(.data), exprs))

    oldfile <- tblFile(.data)
    if(hasTblFile(.data))
        on.exit(file.remove(oldfile))

    # need to use rxImport on non-Xdf data sources because of bugs in rxDataStep
    cl <- if(inherits(.data, "RxXdfData"))
        substitute(rxDataStep(.data, newTblFile(), varsToKeep=.expr, overwrite=TRUE),
            list(.expr=vars))
    else substitute(rxImport(.data, newTblFile(), varsToKeep=.expr, overwrite=TRUE),
            list(.expr=vars))
    cl[names(rxArgs)] <- rxArgs

    .data <- tbl(eval(cl), file=NULL, hasTblFile=TRUE)

    if(is.null(grps))
        .data
    else group_by_(.data, .dots=grps)
}

