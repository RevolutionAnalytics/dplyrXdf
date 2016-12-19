#' Select columns in a data source
#'
#' @param .data A data source.
#' @param ... Unquoted variables to select.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#'
#' @details
#' All the special functions mentioned in the documentation for \code{\link[dplyr]{select}} will also work with dplyrXdf. Note that renaming a variable is \emph{not} supported in dplyrXdf \code{select}. If you want to do this, follow the \code{select} with a \code{rename}; the latter is very fast, as it only modifies the metadata portion of an xdf file, not the data itself.
#'
#' @return
#' An object representing the transformed data. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[dplyr]{select}} in package dplyr
#' @rdname select
#' @aliases select select_
#' @export
select_.RxFileData <- function(.data, ..., .outFile, .rxArgs, .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.outFile)) .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs

    grps <- groups(.data)
    vars <- c(grps, select_vars_(names(.data), lapply(exprs, lazyeval::as.lazy, dots$env)))
    if(length(vars) == 0)
        stop("No variables selected", call.=FALSE)

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    .outFile <- createOutput(.data, .outFile)

    # need to use rxImport on non-Xdf data sources because of bugs in rxDataStep
    cl <- if(inherits(.data, "RxXdfData"))
        substitute(rxDataStep(.data, .outFile, varsToKeep=.expr, overwrite=TRUE),
            list(.expr=vars))
    else substitute(rxImport(.data, .outFile, varsToKeep=.expr, overwrite=TRUE),
            list(.expr=vars))
    cl[names(.rxArgs)] <- .rxArgs

    .data <- eval(cl)
    simpleRegroup(.data, grps)
}

