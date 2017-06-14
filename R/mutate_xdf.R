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
#' @aliases mutate transmute
#' @rdname mutate
#' @export
mutate.RxFileData <- function(.data, ..., .outFile=tbl_xdf(.data), .rxArgs)
{
    dots <- rlang::quos(..., .named=TRUE)
    transforms <- lapply(dots, rlang::get_expr)
    # turn a list of quoted expressions into a quoted list of expressions
    transforms <- if(length(transforms) > 0)
        as.call(c(as.name("list"), transforms))
    else NULL

    arglst <- list(.data, transforms=transforms)
    arglst <- doExtraArgs(arglst, .data, rlang::enexpr(.rxArgs), .outFile)

    on.exit(deleteIfTbl(.data))
    rlang::invoke("rxDataStep", arglst, .env=parent.frame(), .bury=NULL)
}


#' @rdname mutate
#' @export
mutate.grouped_tbl_xdf <- function(.data, ..., .outFile=tbl_xdf(.data), .rxArgs)
{
    stopIfHdfs(.data, "mutate on grouped data not supported on HDFS")

    dots <- rlang::quos(..., .named=TRUE)
    transforms <- lapply(dots, rlang::get_expr)
    # turn a list of quoted expressions into a quoted list of expressions
    transforms <- if(length(transforms) > 0)
        as.call(c(as.name("list"), transforms))
    else NULL

    grps <- group_vars(.data)
    if(any(names(transforms) %in% grps))
        stop("cannot mutate grouping variable")

    arglst <- list(.data, transforms=transforms)
    arglst <- doExtraArgs(arglst, .data, rlang::enexpr(.rxArgs), .outFile)

    xdflst <- splitGroups(.data)
    on.exit(deleteIfTbl(xdflst))
    outlst <- createSplitOutput(xdflst, .outFile)
    outlst <- rxExec(function(data, output, arglst) {
        arglst[[1]] <- data
        arglst$outFile <- output
        rlang::invoke("rxDataStep", arglst, .env=parent.frame(), .bury=NULL)
    }, data=rxElemArg(xdflst), output=rxElemArg(outlst), arglst, packagesToLoad="dplyr")

    combineGroups(outlst, .outFile, grps)
}
