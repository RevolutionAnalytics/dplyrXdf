#' Do arbitrary operations on a tbl
#'
#' The \code{do} verb converts the data to a data frame before running the operations. The \code{doXdf} verb keeps the data in Xdf format, so is not (as) limited by memory.
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param ... Expressions to apply.
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
#' @aliases do
#' @rdname do
#' @export
do_.RxFileData <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # identify Revo-specific arguments
    if(any(names(dots) == ".rxArgs"))
    {
        warning("do doesn't support .rxArgs argument", call.=FALSE)
        dots[[".rxArgs"]] <- NULL
    }

    check_named_args(dots)
    oldData <- tblSource(.data)
    on.exit(if(hasTblFile(.data))
        deleteTbl(oldData))
    do_base(.data, dots=dots)
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
doXdf_.RxFileData <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)

    check_named_args(dots)
    oldData <- tblSource(.data)
    on.exit(deleteTbl(oldData))
    doXdf_base(.data, dots$exprs, grps=NULL, dots$rxArgs, dots$env)
}


#' @details
#' To run expressions on a grouped Xdf tbl, \code{do} and \code{doXdf} split the data into one file per group, and the arguments are called on each file. This ensures that the groups will be appropriately generated regardless of the types of the grouping variables. Note however this may be slow if you have a large number of groups; and, for \code{do}, the operation will be limited by memory if the number of rows per group is large.
#' @rdname do
#' @export
do_.grouped_tbl_xdf <- function(.data, ..., .dots)
{
    stopIfHdfs(x, "do on grouped data not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ...)

    # identify Revo-specific arguments
    if(any(names(dots) == ".rxArgs"))
    {
        warning("do doesn't support .rxArgs argument", call.=FALSE)
        dots[[".rxArgs"]] <- NULL
    }

    check_named_args(dots)
    grps <- groups(.data)

    oldData <- tblSource(.data)
    hasTbl <- hasTblFile(.data)
    on.exit({
        deleteTbl(xdflst)
        if(hasTbl) deleteTbl(oldData)
    })

    xdflst <- split_groups(.data, .data)
    dolst <- rxExec(do_base, data=rxElemArg(xdflst), dots, grps, packagesToLoad="dplyrXdf")
    df <- bind_rows(dolst)

    ngroupvars <- length(groups(.data))
    if(ngroupvars > 1)
        group_by_(df, .dots=groups(.data))  # don't strip off one level of grouping
    else df
}


#' @rdname do
#' @export
doXdf_.grouped_tbl_xdf <- function(.data, ..., .dots)
{
    stopIfHdfs(x, "doXdf on grouped data not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ...)
    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    check_named_args(exprs)
    grps <- groups(.data)

    oldData <- tblSource(.data)
    hasTbl <- hasTblFile(.data)
    on.exit({
        deleteTbl(xdflst)
        if(hasTbl) deleteTbl(oldData)
    })

    xdflst <- split_groups(.data, oldData)
    dolst <- rxExec(doXdf_base, data=rxElemArg(xdflst), exprs, grps, rxArgs, dots$env,
        execObjects="check_named_args", packagesToLoad="dplyrXdf")
    df <- bind_rows(dolst)

    ngroupvars <- length(groups(.data))
    if(ngroupvars > 1)
        group_by_(df, .dots=groups(.data))  # don't strip off one level of grouping
    else df
}


do_base <- function(data, dots, grps=NULL)
{
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
doXdf_base <- function(data, exprs, grps=NULL, rxArgs, env)
{
    named <- check_named_args(exprs)
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
