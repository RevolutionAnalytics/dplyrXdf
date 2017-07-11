#' Rename columns in an Xdf file
#'
#' @param .data A data source.
#' @param ... Key-value pairs of unquoted variables to rename, in new = old format.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
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
#' @aliases rename
#' @export
rename.RxFileData <- function(.data, ..., .outFile=tbl_xdf(.data), .rxArgs)
{
    # renaming broken on MRS <= 9.1
    if(isCompositeXdf(.data) && packageVersion("RevoScaleR") <= package_version("9.1.0"))
        stop("renaming not supported for composite Xdf files", call.=FALSE)

    dots <- rlang::quos(..., .named=TRUE)

    grps <- group_vars(.data)
    vars <- rename_vars(names(.data), !!!dots)

    ## permutations of input -> output
    # filesrc -> df          rxDataStep
    # filesrc -> xdf         rxDataStep
    # filesrc -> tbl_xdf     rxDataStep
    # xdf -> df              rxDataStep
    # xdf -> xdf             rxDataStep
    # xdf -> tbl_xdf         rxDataStep
    # tbl_xdf -> df          rxDataStep
    # tbl_xdf -> xdf         rxDataStep
    # tbl_xdf -> tbl_xdf     in-place if rxArgs not supplied, rxDataStep otherwise
    if(!(missing(.outFile) &&
         inherits(.data, "tbl_xdf") &&
         .data@hasTblFile &&
         missing(.rxArgs)))
    {
        arglst <- list(.data)
        arglst <- doExtraArgs(arglst, .data, rlang::enexpr(.rxArgs), .outFile)

        oldData <- .data
        .data <- callRx("rxDataStep", arglst)
        on.exit(deleteIfTbl(oldData))
    }

    names(.data) <- names(vars)
    # check if grouping vars were renamed -- but only if output is data frame or tbl_xdf 
    if(length(grps) > 0 && inherits(.data, c("data.frame", "tbl_xdf")))
    {
        renamed <- vars[vars != names(vars)]
        renamed <- grps %in% names(renamed)
        grps[renamed] <- vars[renamed]
        group_by_at(.data, grps)
    }
    else .data
}


