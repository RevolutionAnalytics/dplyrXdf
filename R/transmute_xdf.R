#' @include mutate_xdf.R
NULL


#' @rdname mutate
#' @export
transmute.RxFileData <- function(.data, ..., .outFile, .rxArgs)
{
    dots <- quos(..., .named=TRUE)

    transforms <- lapply(dots, get_expr)
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
    rlang::invoke("rxDataStep", arglst)
}


#' @rdname mutate
#' @export
transmute.grouped_tbl_xdf <- function(.data, ..., .outFile, .rxArgs)
{
    stopIfHdfs(.data, "mutate on grouped data not supported on HDFS")

    dots <- quos(..., .named=TRUE)

    transforms <- lapply(dots, get_expr)
    # turn a list of quoted expressions into a quoted list of expressions
    transforms <- if(length(transforms) > 0)
        as.call(c(as.name("list"), transforms))
    else NULL

    if(any(sapply(transforms, is.null)))
        stop("do not set variables to NULL in transmute; to delete variables, leave them out of the function call")

    # piping messes up NSE
    arglst <- list(get_expr(rlang::enquo(.data)), transforms=transforms)
    arglst <- doExtraArgs(arglst, .data, rlang::enexpr(.rxArgs), .outFile)
    arglst <- setTransmuteVars(names(.data), grps)

    grps <- groups(.data)
    if(any(names(transforms) %in% grps))
        stop("cannot mutate grouping variable")
    xdflst <- split_groups(.data)
    outlst <- createSplitOutput(xdflst, .outFile)

    outlst <- rxExec(function(data, output, arglst) {
        arglst[[1]] <- data
        arglst$outFile <- output
        rlang::invoke("rxDataStep", arglst)
    }, data=rxElemArg(xdflst), output=rxElemArg(outlst), arglst, packagesToLoad="dplyrXdf", execObjects="deleteIfTbl")

    combineGroups(outlst, .outFile, grps)
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
        dropvars <- setdiff(vars, c(names(arglst$transforms), grps))
        arglst$varsToDrop <- dropvars
    }
    arglst
}
