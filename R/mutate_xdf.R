#' Add or modify variables
#'
#' @description
#' Use \code{mutate} to add new variables and preserve existing ones; use \code{transmute} to keep only new and modified variables.
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param ... Variables to add or modify.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details. See "Details" below for information on passing \code{\link[RevoScaleR]{rxTransform}} arguments.
#'
#' @details
#' These functions call \code{\link[RevoScaleR]{rxDataStep}} to do the variable transformations. For simple transformations, namely those that might be done using \code{rxDataStep}'s \code{transforms} argument, you can simply pass these as named arguments in the main \code{mutate} or \code{transmute} call. More complex transformations can be passed in a \code{.rxArgs} argument, which should be a named list containing one or more of the \code{transformFunc}, \code{transformVars}, \code{transformObjects}, \code{transformPackages} and \code{transformEnvir} parameters.
#'
#' Note that if you supply a \code{transformFunc}, its returned variables will override any transformations in the main call to \code{mutate} and \code{transmute}). In particular, the results of any such inline transformations will be lost unless you also include them in the output of the \code{transformFunc}. This mirrors the existing behaviour of the variable transformation functionality in RevoScaleR. It's not recommended to use both inline transformations and a \code{transformFunc} at the same time, as the results may be confusing.
#'
#' To modify a grouped Xdf tbl, these functions create a factor variable \code{.group.} split the data into one file per group, and call \code{rxDataStep} on each file. This ensures two things: first, that the groups will be appropriately generated regardless of the types of the grouping variables; and second, the code remains scalable to large dataset sizes. Note however that this may be slow if you have a large number of groups.
#'
#' @seealso
#' \code{\link[dplyr]{mutate}} and \code{\link{transmute}} in package dplyr, \code{\link[RevoScaleR]{rxDataStep}}, \code{\link[RevoScaleR]{rxTransform}} for variable transformations in RevoScaleR
#' @aliases mutate transmute
#' @rdname mutate
#' @export
mutate_.RxFileData <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs
    
    tbl(mutate_base(.data, exprs, rxArgs), file=NULL, hasTblFile=TRUE)
}


#' @export
mutate_.grouped_tbl_xdf <- function(.data, ..., .dots)
{
    stopIfHdfs(.data, "mutate on grouped data not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs
    
    if(any(names(exprs) %in% groups(.data)))
        stop("cannot mutate grouping variable")

    outSource <- tblSource(.data)
    xdflst <- split_groups(.data, outSource)
    xdflst <- rxExec(mutate_base, data=rxElemArg(xdflst), exprs, rxArgs, groups(.data), tblDir=tempdir(),
        execObjects=c("deleteTbl", "newTbl"), packagesToLoad="dplyrXdf")
    combine_groups(xdflst, .data, groups(.data))
}


mutate_base <- function(data, exprs, rxArgs=NULL, gvars=NULL, tblDir=tempdir())
{
    oldData <- data
    if(hasTblFile(data))
        on.exit(deleteTbl(oldData))

    # write to new file to avoid issues with deleting variables
    exprlst <- if(length(exprs) > 0)
        as.call(c(quote(list), exprs))
    else NULL
    cl <- substitute(rxDataStep(data, newTbl(data, tblDir=tblDir), transforms=.expr),
        list(.expr=exprlst))

    if(!is.null(rxArgs))
    {
        # make sure to keep grouping variable, if present
        # do this by _removing_ it from the transformVars parameter
        if(!is.null(gvars))
        {
            rxArgs$transformVars <- if(!is.null(rxArgs$transformVars))
                setdiff(rxArgs$transformVars, gvars)
            else setdiff(union(names(data), names(exprs)), gvars)
        }

        cl[names(rxArgs)] <- rxArgs
    }
    eval(cl)
}


identify_rxTransform_args <- function(dots)
{
    rxNames <- c("transformObjects", "transformFunc", "transformVars", "transformPackages", "transformEnvir")
    exprs <- lapply(dots, "[[", "expr")
    if(!is.null(exprs$.rxArgs))
    {
        rxArgs <- eval(exprs$.rxArgs)  # dicey...
        if(!all(names(rxArgs) %in% rxNames))
            stop("unknown rxTransform argument")
        exprs[[".rxArgs"]] <- NULL
    }
    else rxArgs <- NULL
    list(rxArgs=rxArgs, exprs=exprs)
}

