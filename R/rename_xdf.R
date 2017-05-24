#' Rename columns in an Xdf file
#'
#' @param .data A data source.
#' @param ... Key-value pairs of unquoted variables to rename, in new = old format.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#'
#' @details
#' Renaming is generally a very fast operation, as only the metadata in the Xdf file header is changed. The exception is when the dataset needs to be copied, either to preserve dplyrXdf's semantics of never modifying the source data for a pipeline, or if the the desired output format is a data frame or persistent Xdf file. If a copy is made, \code{rxDataStep} is used and will accept arguments supplied in \code{.rxArgs}.
#'
#' @return
#' An object representing the transformed data. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[dplyr]{rename}} in package dplyr
#' @rdname rename
#' @aliases rename rename_
#' @export
rename_.RxFileData <- function(.data, ..., .outFile, .rxArgs, .dots)
{
    dots <- lazyeval::all_dots(.dots, ...)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.outFile)) .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs

    grps <- group_vars(.data)
    vars <- rename_vars_(names(.data), exprs)

    .outFile <- createOutput(.data, .outFile)

    ## permutations of input -> output
    # filesrc -> df          rxDataStep
    # filesrc -> xdf         rxDataStep
    # filesrc -> tbl_xdf     rxDataStep
    # xdf -> df              rxDataStep
    # xdf -> xdf             rxDataStep
    # xdf -> tbl_xdf         rxDataStep
    # tbl_xdf -> df          rxDataStep
    # tbl_xdf -> xdf         rxDataStep
    # tbl_xdf -> tbl_xdf     in-place
    if(!(hasTblFile(.data) && inherits(.outFile, "tbl_xdf")))
    {
        oldData <- .data
        if(hasTblFile(.data))
            on.exit(deleteTbl(oldData))

        cl <- quote(rxDataStep(.data, .outFile, overwrite=TRUE))
        cl[names(.rxArgs)] <- .rxArgs
        .data <- eval(cl)
    }

    names(.data) <- names(vars)
    # check if grouping vars were renamed -- but only if output is data frame or tbl_xdf 
    if(!is.null(grps) && inherits(.data, c("data.frame", "tbl_xdf")))
    {
        renamed <- vars[vars != names(vars)]
        renamed <- grps %in% names(renamed)
        grps[renamed] <- vars[renamed]
        group_by_(.data, .dots=grps)
    }
    else .data
}


