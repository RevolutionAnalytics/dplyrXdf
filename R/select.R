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
#' \code{\link[dplyr]{select}} in package dplyr, \code{\link{subset}}
#'
#' @examples
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#' tbl <- select(mtx, mpg, cyl)
#' names(tbl)
#'
#' # filter on rows and columns simultaneously with .rxArgs
#' tbl2 <- select(mtx, mpg, cyl, .rxArgs=list(rowSelection=am == 1 & vs == 1))
#' dim(tbl2)
#'
#' # save to a persistent Xdf file
#' select(mtx, mpg, cyl, .outFile="mtcars_select.xdf")
#' @rdname select
#' @aliases select
#' @export
select.RxFileData <- function(.data, ..., .outFile=tbl_xdf(.data), .rxArgs)
{
    grps <- group_vars(.data)
    vars <- unique(c(grps, select_vars(names(.data), ...)))
    if(length(vars) == 0)
        stop("No variables selected", call.=FALSE)

    arglst <- list(.data, varsToKeep=vars)
    arglst <- doExtraArgs(arglst, .data, enexpr(.rxArgs), .outFile)

    on.exit(deleteIfTbl(.data))
    # need to use rxImport on non-Xdf data sources because of bugs in rxDataStep (?)
    output <- callRx("rxDataStep", arglst)
    simpleRegroup(output, grps)
}

