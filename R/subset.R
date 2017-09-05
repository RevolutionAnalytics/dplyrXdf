#' Subset a data source by rows and/or columns
#'
#' @param .data A data source object, or tbl wrapping the same.
#' @param subset Logical expression indicating rows to keep.
#' @param select Columns to select. See \code{link[dplyr]{select}} for the ways in which you can keep or drop columns.
#' @param ... Other arguments passed to lower-level functions.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#' @details
#' This is a method for the \code{\link[base]{subset}} generic from base R. It combines the effects of the \code{filter} and \code{select} verbs, allowing you to subset a RevoScaleR data source (typically an xdf file) by rows and columns simultaneously. The advantage of this for an Xdf file is that it significantly reduces the amount of I/O compared to doing the row and column subsetting in separate steps.
#'
#' @return
#' An object representing the subsetted data. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[base]{subset}} in base R, \code{\link{filter}}, \code{\link{select}}, \code{\link[RevoScaleR]{rxDataStep}}
#'
#' @examples
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#' tbl <- subset(mtx, mpg > 20, c(mpg, cyl))
#' dim(tbl)
#'
#' # transform and filter simultaneously with .rxArgs
#' tbl2 <- subset(mtx, mpg > 20, c(mpg, cyl), .rxArgs=list(transforms=list(mpg2=2 * mpg)))
#' dim(tbl2)
#' names(tbl2)
#'
#' # save to a persistent Xdf file
#' subset(mtx, mpg > 20, c(mpg, cyl), .outFile="mtcars_subset.xdf")
#' @aliases subset
#' @rdname subset
#' @export
subset.RxFileData <- function(.data, subset=NULL, select=NULL, .outFile=tbl_xdf(.data), .rxArgs)
{
    grps <- group_vars(.data)
    select <- rlang::get_expr(rlang::enquo(select))
    if(is.null(select))
        select <- names(.data)
    
    vars <- do.call(dplyr::select_vars, c(list(names(.data), select)),
                    envir=parent.frame()) # cannot use rlang::invoke
    vars <- unique(c(grps, vars))
    if(length(vars) == 0)
        stop("No variables selected", call.=FALSE)

    rows <- rlang::get_expr(rlang::enquo(subset))

    arglst <- list(.data, rowSelection=rows, varsToKeep=vars)
    arglst <- doExtraArgs(arglst, .data, rlang::enexpr(.rxArgs), .outFile)

    on.exit(deleteIfTbl(.data))
    output <- callRx("rxDataStep", arglst)
    simpleRegroup(output, grps)
}


