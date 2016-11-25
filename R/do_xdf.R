#' Do arbitrary operations on a tbl
#'
#' The \code{do} verb converts the data to a data frame before running the operations. The \code{doXdf} verb keeps the data in Xdf format, so is not (as) limited by memory.
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param ... Expressions to apply.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#'
#' @details
#' The difference between the \code{do} and \code{doXdf} verbs is that the former converts the data into a data frame before running the expressions on it; while the latter passes the data as Xdf files. \code{do} is thus more flexible in the expressions it can run (basically anything that works with data frames), whereas \code{doXdf} is better able to handle large datasets. The final output from \code{doXdf} must still be able to fit in memory (see below).
#'
#' @return
#' The \code{do} and \code{doXdf} verbs always return a data frame, unlike the other verbs for Xdf objects. This is because they are meant to execute code that can return arbitrarily complex objects, and Xdf files can only store atomic data.
#'
#' @seealso
#' \code{\link[dplyr]{do}} in package dplyr
#' @aliases do do_
#' @rdname do
#' @export
do_.RxFileData <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots, fromDo=TRUE)
    exprs <- dots$exprs
    .outFile <- dots$output
    .rxArgs <- dots$rxArgs

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    if(!is.null(.rxArgs))
    {
        warning("do() doesn't support .rxArgs argument", call.=FALSE)
        .rxArgs <- NULL
    }
    if(!is.null(.outFile) && !is.na(.outFile))
        warning("do() only outputs data frames", call.=FALSE)

    named <- check_named_args(dots)
    do_base(.data, exprs, grps=NULL, dots$env)
}


#' @rdname do
#' @export
doXdf <- function (.data, ...) 
{
    doXdf_(.data, .dots = lazyeval::lazy_dots(...))
}


#' @rdname do
#' @export
doXdf_ <- function(.data, ..., .dots)
{
    UseMethod("doXdf_")
}


#' @rdname do
#' @export
doXdf_.RxFileData <- function(.data, ..., .rxArgs, .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots, fromDo=TRUE)
    exprs <- dots$exprs
    .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    if(!is.null(.outFile) && !is.na(.outFile))
        warning("doXdf() only outputs data frames", call.=FALSE)

    named <- check_named_args(dots)
    doXdf_base(.data, exprs, grps=NULL, .rxArgs, dots$env, named)
}


#' @details
#' To run expressions on a grouped Xdf tbl, \code{do} and \code{doXdf} split the data into one file per group, and the arguments are called on each file. This ensures that the groups will be appropriately generated regardless of the types of the grouping variables. Note however this may be slow if you have a large number of groups; and, for \code{do}, the operation will be limited by memory if the number of rows per group is large.
#' @rdname do
#' @export
do_.grouped_tbl_xdf <- function(.data, ..., .outFile, .dots)
{
    stopIfHdfs(.data, "do on grouped data not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ...)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots, fromDo=TRUE)
    exprs <- dots$exprs
    .outFile <- dots$output
    .rxArgs <- dots$rxArgs

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    if(!is.null(.rxArgs))
    {
        warning("do() doesn't support .rxArgs argument", call.=FALSE)
        .rxArgs <- NULL
    }
    if(!is.null(.outFile) && !is.na(.outFile))
        warning("do() only outputs data frames", call.=FALSE)

    named <- check_named_args(exprs)
    grps <- groups(.data)

    on.exit(deleteTbl(xdflst), add=TRUE)
    xdflst <- split_groups(.data, .data)
    dolst <- rxExec(do_base, data=rxElemArg(xdflst), exprs, grps, dots$env, packagesToLoad="dplyrXdf")
    df <- bind_rows(dolst)

    # mimic grouping behaviour of do for data frames
    if(length(grps) == 0)
        df
    else if(named)
        rowwise(df)
    else group_by_(df, .dots=grps)
}


#' @rdname do
#' @export
doXdf_.grouped_tbl_xdf <- function(.data, ..., .outFile, .rxArgs, .dots)
{
    stopIfHdfs(.data, "doXdf on grouped data not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ...)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots, fromDo=TRUE)
    exprs <- dots$exprs
    .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    if(!is.null(.outFile) && !is.na(.outFile))
        warning("doXdf() only outputs data frames", call.=FALSE)

    named <- check_named_args(exprs)
    grps <- groups(.data)

    on.exit(deleteTbl(xdflst), add=TRUE)
    xdflst <- split_groups(.data)
    dolst <- rxExec(doXdf_base, data=rxElemArg(xdflst), exprs, grps, .rxArgs, dots$env, named,
                    packagesToLoad="dplyrXdf")
    df <- bind_rows(dolst)

    # mimic grouping behaviour of do for data frames
    if(length(grps) == 0)
        df
    else if(named)
        rowwise(df)
    else group_by_(df, .dots=grps)
}


do_base <- function(data, exprs, grps=NULL, env)
{
    dots <- sapply(exprs, function(expr) {
        lazyeval::as.lazy(expr, env)
    }, simplify=FALSE)
    data <- as.data.frame(data)
    out <- do_(data, .dots=dots)
    if(!is.null(grps))
    {
        grps <- data[rep(1, nrow(out)), grps, drop=FALSE]
        cbind(grps, out)
    }
    else out
}


# copy functionality of dplyr:::do_.data.frame, except dots must be named
doXdf_base <- function(data, exprs, grps=NULL, rxArgs, env, named)
{
    datlst <- list(.=data)
    if(named)
    {
        out <- lapply(exprs, function(arg) {
            arg[names(rxArgs)] <- rxArgs
            arg <- lazyeval::as.lazy(arg, env)
            list(lazyeval::lazy_eval(arg, datlst))
        })
        attr(out, "row.names") <- .set_row_names(1L)
        names(out) <- names(exprs)
    }
    else
    {
        expr <- lazyeval::as.lazy(exprs[[1]], env)
        out <- as.data.frame(lazyeval::lazy_eval(expr, datlst))
    }
    class(out) <- c("tbl_df", "data.frame")
    if(!is.null(grps))
    {
        grps <- head(data, 1)[rep(1, nrow(out)), grps, drop=FALSE]
        cbind(grps, out[names(out) != ".group."])
    }
    else out
}


# copied from dplyr:::named_args: check that all args are named, or there is exactly 1 unnamed, 0 named
check_named_args <- function (args) 
{
    names2 <- if(is.null(names(args))) rep("", length(args)) else names(args)
    named <- sum(names2 != "")
    if (!(named == 0 || named == length(args))) {
        stop("Arguments to do() must either be all named or all unnamed", 
            call. = FALSE)
    }
    if (named == 0 && length(args) > 1) {
        stop("Can only supply single unnamed argument to do()", 
            call. = FALSE)
    }
    if (named == 1 && names(args) == ".f") {
        stop("do syntax changed in dplyr 0.2. Please see documentation for details", 
            call. = FALSE)
    }
    named != 0
}
