#' @include mutate_xdf.R
NULL


#' @rdname mutate
#' @export
transmute_.RxFileData <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    if(any(sapply(exprs, is.null)))
        stop("do not set variables to NULL in transmute; to delete variables, leave them out of the function call")

    tbl(transmute_base(.data, exprs, rxArgs), file=NULL, hasTblFile=TRUE)
}


#' @rdname mutate
#' @export
transmute_.grouped_tbl_xdf <- function(.data, ..., .dots)
{
    stopIfHdfs(.data, "transmute on grouped data not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    if(any(sapply(exprs, is.null)))
        stop("do not set variables to NULL in transmute; to delete variables, leave them out of the function call")
    if(any(names(exprs) %in% groups(.data)))
        stop("cannot transmute grouping variable")

    outData <- tblSource(.data)
    xdflst <- split_groups(.data, outData)
    xdflst <- rxExec(transmute_base, data=rxElemArg(xdflst), exprs, rxArgs, groups(.data), tblDir=tempdir(),
        execObjects=c("deleteTbl", "newTbl"), packagesToLoad="dplyrXdf")
    combine_groups(xdflst, outData, groups(.data))
}


transmute_base <- function(data, exprs, rxArgs=NULL, gvars=NULL, tblDir=tempdir())
{
    # identify variables to drop
    if(!is.null(rxArgs$transformFunc))  # first case: transformFunc is present
    {
        # pad out transformVars parameter with all variables in dataset (excluding grouping variables)
        # this will force rxDataStep to drop vars not returned from transformFunc
        dropvars <- setdiff(union(names(data), names(exprs)), gvars)
        rxArgs$transformVars <- dropvars
    }
    else  # second case: no transformFunc
    {
        # set variables to NULL to drop them
        dropvars <- setdiff(names(data), c(names(exprs), gvars))
        exprs[dropvars] <- list(NULL)
    }

    exprlst <- if(length(exprs) > 0)
        as.call(c(quote(list), exprs))
    else NULL

    oldData <- tblSource(data)
    if(hasTblFile(data))
        on.exit(deleteTbl(oldData))
    cl <- substitute(rxDataStep(data, newTbl(data, tblDir=tblDir), transforms=.expr),
        list(.expr=exprlst))
    cl[names(rxArgs)] <- rxArgs

    eval(cl)
}

