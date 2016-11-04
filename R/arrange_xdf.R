#' @export
arrange_.RxFileData <- function(.data, ..., .output, .rxArgs, .dots)
{
    stopIfHdfs(.data, "arrange not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # .output and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.output)) .output <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs
    grps <- groups(.data)

    # rxSort only works with xdf files, not other data sources
    if(!inherits(.data, "RxXdfData"))
        .data <- tbl(.data)

    exprs <- c(lapply(grps, as.name), exprs)
    desc <- sapply(exprs, function(e) !is.name(e) && identical(e[[1]], as.name("desc")))
    vars <- sapply(exprs, function(e) all.vars(e)[1])

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    .output <- createOutput(.data, .output)
    cl <- quote(rxSort(.data, .output, sortByVars=vars, decreasing=desc, overwrite=TRUE))
    cl[names(.rxArgs)] <- .rxArgs

    .data <- eval(cl)
    simpleRegroup(.data, grps)
}



