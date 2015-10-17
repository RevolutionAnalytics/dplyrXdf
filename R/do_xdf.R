#' Do arbitrary operations on a tbl
#'
#' The \code{do} verb converts the data to a data frame before running the operations. The \code{doXdf} verb keeps the data in Xdf format, so is not limited by memory.
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param ... Expressions to apply.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#'
#' @return
#' The \code{do} and \code{doXdf} verbs always return a data frame, unlike the other verbs for Xdf objects. 
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

    oldfile <- tblFile(.data)
    on.exit(if(hasTblFile(.data))
        file.remove(oldfile))
    do_base(.data, dots=dots)
}


#' @details
#' The \code{do} verb supports named and unnamed arguments, as per \code{dplyr::do}. The \code{doXdf} verb only supports named arguments.
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

    oldfile <- tblFile(.data)
    on.exit(if(hasTblFile(.data))
        file.remove(oldfile))
    doXdf_base(.data, dots$exprs, grps=NULL, dots$rxArgs, dots$env)
}


#' @details
#' To run expressions on a grouped Xdf tbl, \code{do} and \code{doXdf} create a factor variable \code{.group.} containing all observed combinations of the grouping variables. The data is then split into multiple files by this factor, and the arguments are called on each file. This ensures that the groups will be appropriately generated regardless of the types of the grouping variables. Note however that this may be slow if you have a large number of groups; and, for \code{do}, the operation will be limited by memory if the number of rows per group is large.
#' @rdname do
#' @export
do_.grouped_tbl_xdf <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # identify Revo-specific arguments
    if(any(names(dots) == ".rxArgs"))
    {
        warning("do doesn't support lib.rxArgs argument", call.=FALSE)
        dots[[".rxArgs"]] <- NULL
    }

    grps <- groups(.data)

    oldfile <- tblFile(.data)
    hasTbl <- hasTblFile(.data)
    on.exit({
        file.remove(names(xdflst))
        if(hasTbl)
            file.remove(oldfile)
    })

    xdflst <- split_groups(.data, oldfile)
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
    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    grps <- groups(.data)

    oldfile <- tblFile(.data)
    hasTbl <- hasTblFile(.data)
    on.exit({
        file.remove(names(xdflst))
        if(hasTbl)
            file.remove(oldfile)
    })

    xdflst <- split_groups(.data, oldfile)
    dolst <- rxExec(doXdf_base, data=rxElemArg(xdflst), exprs, grps, rxArgs, dots$env, packagesToLoad="dplyrXdf")
    df <- bind_rows(dolst)

    ngroupvars <- length(groups(.data))
    if(ngroupvars > 1)
        group_by_(df, .dots=groups(.data))  # don't strip off one level of grouping
    else df
}


do_base <- function(data, dots, grps=NULL)
{
    data <- as.data.frame(data)
    df <- do_(data, .dots=dots)
    if(!is.null(grps))
        cbind(data[1, grps, drop=FALSE], df)
    else df
}


# copy functionality of dplyr:::do_.data.frame, except dots must be named
doXdf_base <- function(data, exprs, grps=NULL, rxArgs, env)
{
    datlst <- list(.=data)
    out <- lapply(exprs, function(arg) {
        arg[names(rxArgs)] <- rxArgs
        arg <- lazyeval::as.lazy(arg, env)
        list(lazyeval::lazy_eval(arg, datlst))
    })
    names(out) <- names(exprs)
    attr(out, "row.names") <- .set_row_names(1L)
    class(out) <- c("tbl_df", "data.frame")
    if(!is.null(grps))
        cbind(head(data, 1)[grps], out)
    else out    
}

