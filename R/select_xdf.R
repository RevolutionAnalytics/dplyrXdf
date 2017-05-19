#' Select columns in a data source
#'
#' @param .data A data source.
#' @param ... Unquoted variables to select.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
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
select.RxFileData <- function(.data, ..., .outFile, .rxArgs)
{
    grps <- groups(.data)
    vars <- c(grps, select_vars(names(.data), ...))
    if(length(vars) == 0)
        stop("No variables selected", call.=FALSE)

    oldData <- .data
    if(hasTblFile(.data))
        on.exit(deleteTbl(oldData))

    arglst <- list(.data, varsToKeep=vars)
    arglst <- doExtraArgs(arglst, .data, enexpr(.rxArgs), .outFile)

    # need to use rxImport on non-Xdf data sources because of bugs in rxDataStep
    .data <- if(inherits(.data, "RxXdfData"))
        invoke("rxDataStep", arglst)
    else invoke("rxImport", arglst)

    simpleRegroup(.data, grps)
}

