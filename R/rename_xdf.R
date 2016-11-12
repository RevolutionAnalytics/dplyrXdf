#' @export
rename_.RxFileData <- function(.data, ..., .output, .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # .output and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.output)) .output <- dots$output
    .rxArgs <- dots$rxArgs

    if(!is.null(.rxArgs))
    {
        warning("rename() doesn't support .rxArgs argument", call.=FALSE)
        .rxArgs <- NULL
    }

    grps <- groups(.data)
    vars <- rename_vars_(names(.data), exprs)

    .output <- createOutput(.data, .output)

    ## permutations of input -> output
    # filesrc -> df          rxDataStep
    # filesrc -> xdf         rxDataStep
    # filesrc -> tbl_xdf     rxDataStep
    # xdf -> df              rxDataStep
    # xdf -> xdf             rxDataStep
    # xdf -> tbl_xdf         rxDataStep
    # tbl_xdf -> df          rxDataStep
    # tbl_xdf -> xdf         rxDataStep
    # tbl_xdf -> tbl_xdf     in-place
    if(!(hasTblFile(.data) && inherits(.output, "tbl_xdf")))
    {
        oldData <- .data
        if(hasTblFile(.data))
            on.exit(deleteTbl(oldData))

        .data <- rxDataStep(.data, .output)
    }

    names(.data) <- names(vars)
    if(!is.null(grps))  # check if grouping vars were renamed
    {
        renamed <- vars[vars != names(vars)]
        renamed <- grps %in% names(renamed)
        grps[renamed] <- vars[renamed]
        group_by_(.data, .dots=grps)
    }
    else .data
}


