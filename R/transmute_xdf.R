#' @include mutate_xdf.R
NULL


#' @rdname mutate
#' @export
transmute.RxFileData <- function(.data, ..., .outFile=tbl_xdf(.data), .rxArgs)
{
    dots <- rlang::quos(..., .named=TRUE)

    transforms <- lapply(dots, rlang::get_expr)
    # turn a list of quoted expressions into a quoted list of expressions
    transforms <- if(length(transforms) > 0)
        as.call(c(as.name("list"), transforms))
    else NULL

    if(any(sapply(transforms, is.null)))
        stop("do not set variables to NULL in transmute; to delete variables, leave them out of the function call")

    # piping messes up NSE
    arglst <- list(.data, transforms=transforms)
    arglst <- doExtraArgs(arglst, .data, rlang::enexpr(.rxArgs), .outFile)
    arglst <- setTransmuteVars(arglst, names(.data))

    on.exit(deleteIfTbl(.data))
    callRx("rxDataStep", arglst)
}


#' @rdname mutate
#' @export
transmute.grouped_tbl_xdf <- function(.data, ..., .outFile=tbl_xdf(.data), .rxArgs)
{
    stopIfHdfs(.data, "mutate on grouped data not supported on HDFS")

    dots <- rlang::quos(..., .named=TRUE)
    transforms <- lapply(dots, rlang::get_expr)
    # turn a list of quoted expressions into a quoted list of expressions
    transforms <- if(length(transforms) > 0)
        as.call(c(as.name("list"), transforms))
    else NULL
    if(any(sapply(transforms, is.null)))
        stop("do not set variables to NULL in transmute; to delete variables, leave them out of the function call")

    grps <- group_vars(.data)
    if(any(names(transforms) %in% grps))
        stop("cannot mutate grouping variable")

    # piping messes up NSE
    arglst <- list(.data, transforms=transforms)
    arglst <- doExtraArgs(arglst, .data, rlang::enexpr(.rxArgs), .outFile)
    arglst <- setTransmuteVars(arglst, names(.data), grps)

    callGroupedExec(.data, transmutateGrouped, arglst=arglst) %>% 
        combineGroups(.outFile, grps)
}


# identify variables to drop
setTransmuteVars <- function(arglst, vars, grps=NULL)
{
    if(!is.null(arglst$transformFunc)) # complicated case: transformFunc is present
    {
        # pad out transformVars parameter with all variables in dataset (excluding grouping variables)
        # this will force rxDataStep to drop vars not returned from transformFunc
        dropvars <- setdiff(union(vars, names(arglst$transforms)), grps)
        arglst$transformVars <- dropvars
    }
    else
    {
        # set variables to NULL to drop them
        dropvars <- setdiff(vars, c(names(arglst$transforms), grps))
        arglst$transforms[dropvars] <- list(NULL)
    }
    arglst
}
