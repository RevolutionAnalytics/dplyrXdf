#' @export
arrange_.RxFileData <- function(.data, ..., .dots)
{
    stopIfHdfs(.data, "arrange not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    grps <- groups(.data)
    if(!inherits(.data, "RxXdfData"))
        .data <- tbl(.data)

    exprs <- c(lapply(grps, as.name), exprs)
    desc <- sapply(exprs, function(e) !is.name(e) && identical(e[[1]], as.name("desc")))
    vars <- sapply(exprs, function(e) all.vars(e)[1])

    # write to new file to avoid issues with overwriting source    
    oldData <- tblSource(.data)
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    cl <- quote(rxSort(.data, newTbl(.data), sortByVars=vars, decreasing=desc, overwrite=TRUE))
    cl[names(rxArgs)] <- rxArgs

    .data <- tbl(eval(cl), hasTblFile=TRUE)

    if(!is.null(grps))
        group_by_(.data, .dots=grps)
    else .data
}



