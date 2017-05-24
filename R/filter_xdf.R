#' Subset a data source by rows
#'
#' @param .data A data source.
#' @param ... Expressions to filter by.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#' @return
#' An object representing the filtered data. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[dplyr]{filter}} in package dplyr
#' @rdname filter
#' @aliases filter
#' @export
filter.RxFileData <- function(.data, ..., .outFile, .rxArgs)
{
    grps <- group_vars(.data)
    dots <- rlang::quos(..., .named=TRUE)
    #if(length(dots) > 1)
        #env <- rlang::get_env(dots[[1]])
    #else env <- rlang::get_env(caller_env(2))

    exprs <- lapply(dots, rlang::get_expr)
    if(length(exprs) > 0)
    {
        all_exprs <- exprs[[1]]
        if(length(exprs) > 1) for(i in 2:length(exprs))  # loop taken from dplyr:::and_expr
        {
            all_exprs <- substitute(left & right, list(left=all_exprs, right=exprs[[i]]))
        }
    }
    else all_exprs <- NULL

    arglst <- list(.data, rowSelection=all_exprs)
    arglst <- doExtraArgs(arglst, .data, rlang::enexpr(.rxArgs), .outFile)

    on.exit(deleteIfTbl(.data))
    output <- rlang::invoke("rxDataStep", arglst, .env=parent.frame())
    simpleRegroup(output, grps)
}
