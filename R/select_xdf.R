#' Rename columns in an Xdf file
#'
#' @param .data A data source.
#' @param ... Unquoted variables to select.
#' @param .output Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#'
#' @details
#' All the special functions mentioned in the documentation for \code{\link[dplyr]{select}} will also work with dplyrXdf.
#'
#' @return
#' An object representing the transformed data. This depends on the \code{.output} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[dplyr]{select}} in package dplyr
#" @aliases select select_
#' @rdname select
#' @export
select_.RxFileData <- function(.data, ..., .output, .rxArgs, .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # .output and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.output)) .output <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs

    grps <- groups(.data)
    vars <- c(grps, select_vars_(names(.data), exprs))

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    .output <- createOutput(.data, .output)

    # need to use rxImport on non-Xdf data sources because of bugs in rxDataStep
    cl <- if(inherits(.data, "RxXdfData"))
        substitute(rxDataStep(.data, .output, varsToKeep=.expr, overwrite=TRUE),
            list(.expr=vars))
    else substitute(rxImport(.data, .output, varsToKeep=.expr, overwrite=TRUE),
            list(.expr=vars))
    cl[names(.rxArgs)] <- .rxArgs

    .data <- eval(cl)
    simpleRegroup(.data, grps)
}

