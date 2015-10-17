#' @export
filter_.RxFileData <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    grps <- groups(.data)
    all_exprs <- exprs[[1]]
    if(length(exprs) > 1) for(i in 2:length(exprs))  # loop taken from dplyr:::and_expr
    {
        all_exprs <- substitute(left & right, list(left=all_exprs, right=exprs[[i]]))
    }

    oldfile <- tblFile(.data)
    if(hasTblFile(.data))
        on.exit(file.remove(oldfile))
    
    cl <- substitute(rxDataStep(.data, newTblFile(), rowSelection=.expr),
        list(.expr=all_exprs))
    cl[names(rxArgs)] <- rxArgs

    .data <- tbl(eval(cl), file=NULL, hasTblFile=TRUE)

    if(is.null(grps))
        .data
    else group_by_(.data, .dots=grps)
}


