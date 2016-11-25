#' Add or modify variables
#'
#' @description
#' Use \code{mutate} to add new variables and preserve existing ones; use \code{transmute} to keep only new and modified variables.
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param ... Variables to add or modify.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details. See "Details" below for information on passing \code{\link[RevoScaleR]{rxTransform}} arguments.
#'
#' @details
#' These functions call \code{\link[RevoScaleR]{rxDataStep}} to do the variable transformations. For simple transformations, namely those that might be done using \code{rxDataStep}'s \code{transforms} argument, you can simply pass these as named arguments in the main \code{mutate} or \code{transmute} call. More complex transformations can be passed in a \code{.rxArgs} argument, which should be a named list containing one or more of the \code{transformFunc}, \code{transformVars}, \code{transformObjects}, \code{transformPackages} and \code{transformEnvir} parameters.
#'
#' Note that if you supply a \code{transformFunc}, its returned variables will override any transformations in the main call to \code{mutate} and \code{transmute}). In particular, the results of any such inline transformations will be lost unless you also include them in the output of the \code{transformFunc}. This mirrors the existing behaviour of the variable transformation functionality in RevoScaleR. It's not recommended to use both inline transformations and a \code{transformFunc} at the same time, as the results may be confusing.
#'
#' To modify a grouped Xdf tbl, these functions create a factor variable \code{.group.} split the data into one file per group, and call \code{rxDataStep} on each file. This ensures two things: first, that the groups will be appropriately generated regardless of the types of the grouping variables; and second, the code remains scalable to large dataset sizes. Note however that this may be slow if you have a large number of groups.
#'
#' @return
#' An object representing the transformed data. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[dplyr]{mutate}} and \code{\link{transmute}} in package dplyr, \code{\link[RevoScaleR]{rxDataStep}}, \code{\link[RevoScaleR]{rxTransform}} for variable transformations in RevoScaleR
#' @aliases mutate transmute mutate_ transmute_
#' @rdname mutate
#' @export
mutate_.RxFileData <- function(.data, ..., .outFile, .rxArgs, .dots)
{
    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.outFile)) .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs
    
    .outFile <- createOutput(.data, .outFile)
    mutate_base(.data, .outFile, exprs, .rxArgs)
}


#' @rdname mutate
#' @export
mutate_.grouped_tbl_xdf <- function(.data, ..., .outFile, .rxArgs, .dots)
{
    stopIfHdfs(.data, "mutate on grouped data not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.outFile)) .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs

    grps <- groups(.data)    
    if(any(names(exprs) %in% grps))
        stop("cannot mutate grouping variable")

    xdflst <- split_groups(.data)
    outlst <- createSplitOutput(xdflst, .outFile)
    outlst <- rxExec(mutate_base, data=rxElemArg(xdflst), output=rxElemArg(outlst), exprs, .rxArgs, grps,
        execObjects="deleteTbl", packagesToLoad="dplyrXdf")
    combine_groups(outlst, createOutput(.data, .outFile), grps)
}


mutate_base <- function(data, output, exprs, rxArgs=NULL, gvars=NULL)
{
    oldData <- data
    if(hasTblFile(data))
        on.exit(deleteTbl(oldData))

    exprlst <- if(length(exprs) > 0)
        as.call(c(quote(list), exprs))
    else NULL

    cl <- substitute(rxDataStep(data, output, transforms=.expr, overwrite=TRUE),
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

