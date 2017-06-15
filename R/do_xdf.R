#' Do arbitrary operations on a tbl
#'
#' The \code{do} verb converts the data to a data frame before running the operations. The \code{doXdf} verb keeps the data in Xdf format, so is not (as) limited by memory.
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param ... Expressions to apply.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
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
do.RxFileData <- function(.data, ...)
{
    args <- rlang::quos(...)
    if(".rxArgs" %in% names(args))
    {
        warning("do() doesn't use .rxArgs argument", call.=FALSE)
        args$.rxArgs <- NULL
    }
    if(".outFile" %in% names(args))
    {
        warning("do() only outputs data frames", call.=FALSE)
        args$.outFile <- NULL
    }

    named <- checkNamedArgs(args)

    on.exit(deleteIfTbl(.data))
    do(as.data.frame(.data), ...)
}


#' @details
#' To run expressions on a grouped Xdf tbl, \code{do} and \code{doXdf} split the data into one file per group, and the arguments are called on each file. This ensures that the groups will be appropriately generated regardless of the types of the grouping variables. Note however this may be slow if you have a large number of groups; and, for \code{do}, the operation will be limited by memory if the number of rows per group is large.
#' @rdname do
#' @export
do.grouped_tbl_xdf <- function(.data, ...)
{
    stopIfHdfs(.data, "do on grouped data not supported on HDFS")

    args <- rlang::quos(...)
    if(".rxArgs" %in% names(args))
    {
        warning("do() doesn't use .rxArgs argument", call.=FALSE)
        args$.rxArgs <- NULL
    }
    if(".outFile" %in% names(args))
    {
        warning("do() only outputs data frames", call.=FALSE)
        args$.outFile <- NULL
    }

    named <- checkNamedArgs(args)
    grps <- group_vars(.data)

    df <- if(.dxOptions$useExecBy)
        doExecBy(.data, args, grps)
    else
    {
        on.exit(deleteTbl(xdflst))
        xdflst <- splitGroups(.data)
        dolst <- rxExec(doBase, data=rxElemArg(xdflst), args, grps, packagesToLoad="dplyr")
        bind_rows(dolst)
    }

    # mimic grouping behaviour of do for data frames
    if(length(grps) == 0)
        df
    else if(named)
        rowwise(df)
    else group_by_at(df, grps)
}


doBase <- function(.data, exprs, grps=NULL)
{
    .data <- as.data.frame(.data)
    out <- do(.data, !!!exprs)
    if(length(grps) > 0)
    {
        grps <- .data[rep(1, nrow(out)), grps, drop=FALSE]
        cbind(grps, out)
    }
    else out
}


doExecBy <- function(.data, exprs, grps=NULL)
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    dflst <- rxExecBy(.data, grps, function(keys, data, exprs)
        {
            data <- rxDataStep(data, outFile=NULL, maxRowsByCols=NULL) # convert to df
            dplyr::do(data, !!!exprs)
        },
        list(exprs=exprs))
    execByCheck(dflst)
    lapply(dflst, function(res, groupVars)
        {
            grps <- res$keys
            names(grps) <- groupVars
            do.call(cbind, c(grps, list(res$result)))
        }, groupVars=grps) %>%
        bind_rows
}


#' @rdname do
#' @export
do_xdf <- function(.data, ...)
{
    UseMethod("do_xdf")
}


#' @rdname do
#' @export
doXdf <- function(.data, ...)
{
    warning("doXdf is deprecated; use do_xdf instead", call.=FALSE)
    UseMethod("do_xdf")
}


#' @rdname do
#' @export
do_xdf.RxFileData <- function(.data, ...)
{
    args <- rlang::quos(...)
    if(".rxArgs" %in% names(args))
    {
        warning("do_xdf() doesn't use .rxArgs argument", call.=FALSE)
        args$.rxArgs <- NULL
    }
    if(".outFile" %in% names(args))
    {
        warning("do_xdf() only outputs data frames", call.=FALSE)
        args$.outFile <- NULL
    }

    named <- checkNamedArgs(args)

    on.exit(deleteIfTbl(.data))
    doXdfBase(.data, args, grps=NULL, named)
}


#' @rdname do
#' @export
do_xdf.grouped_tbl_xdf <- function(.data, ...)
{
    stopIfHdfs(.data, "do_xdf on grouped data not supported on HDFS")

    args <- rlang::quos(...)
    if(".rxArgs" %in% names(args))
    {
        warning("do_xdf() doesn't use .rxArgs argument", call.=FALSE)
        args$.rxArgs <- NULL
    }
    if(".outFile" %in% names(args))
    {
        warning("do_xdf() only outputs data frames", call.=FALSE)
        args$.outFile <- NULL
    }

    named <- checkNamedArgs(args)
    grps <- group_vars(.data)

    df <- if(.dxOptions$useExecBy)
        doXdfExecBy(.data, args, grps, named)
    else
    {
        on.exit(deleteTbl(xdflst))
        xdflst <- splitGroups(.data)
        dolst <- rxExec(doXdfBase, .data=rxElemArg(xdflst), args, grps, named,
                    packagesToLoad="dplyr")
        bind_rows(dolst)
    }

    # mimic grouping behaviour of do for data frames
    if(length(grps) == 0)
        df
    else if(named)
        rowwise(df)
    else group_by_at(df, grps)
}


# copy functionality of dplyr:::do_.data.frame, except dots must be named
doXdfBase <- function(.data, exprs, grps=NULL, named)
{
    datlst <- rlang::env(.=.data, .data=.data)
    if(named)
    {
        out <- lapply(exprs, function(arg)
        {
            list(rlang::eval_tidy(arg, datlst))
        })
    }
    else
    {
        out <- rlang::eval_tidy(exprs[[1]], datlst)
    }
    out <- as_tibble(out)
    if(length(grps) > 0)
    {
        grps <- head(.data, 1)[rep(1, nrow(out)), grps, drop=FALSE]
        cbind(grps, out[names(out) != ".group."])
    }
    else out
}


doXdfExecBy <- function(.data, exprs, grps, named)
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    dflst <- rxExecBy(.data, grps, function(keys, data, exprs)
        {
            datlst <- rlang::env(.=data, .data=data)
            if(named)
            {
                out <- lapply(exprs, function(arg)
                {
                    list(rlang::eval_tidy(arg, datlst))
                })
            }
            else
            {
                out <- rlang::eval_tidy(exprs[[1]], datlst)
            }
            dplyr::as_tibble(out)
        },
        list(exprs=exprs))
    execByCheck(dflst)
    lapply(dflst, function(res, groupVars)
        {
            grps <- res$keys
            names(grps) <- groupVars
            do.call(cbind, c(grps, list(res$result)))
        }, groupVars=grps) %>%
        bind_rows
}


# copied from dplyr:::named_args: check that all args are named, or there is exactly 1 unnamed, 0 named
checkNamedArgs <- function (args) 
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
    named != 0
}
