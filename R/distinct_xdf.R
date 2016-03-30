#' Select distinct/unique rows
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param ... Variables to use for determining uniqueness.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#'
#' @seealso
#' \code{\link[dplyr]{distinct}} in package dplyr
#' @aliases distinct
#' @rdname distinct
#' @export
distinct_.RxFileData <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    needs_mutate <- vapply(exprs, function(e) !is.name(e), logical(1))
    if(any(needs_mutate))
        .data <- mutate_(.data, .dots=exprs[needs_mutate])

    tbl(distinct_base(.data, names(exprs), rxArgs), file=NULL, hasTblFile=TRUE)
}


#' @details
#' To sort a grouped Xdf tbl, \code{distinct} split the data into one file per group, and calls \code{\link[dplyr]{distinct}} on each. The individual data frames are \code{rbind}ed together and \code{distinct} is called on the overall result. This ensures that the groups will be appropriately generated regardless of the types of the grouping variables. Note however that this may be slow if you have a large number of groups; and the operation will be limited by memory if the number of distinct rows is large.
#' @rdname distinct
#' @export
distinct_.grouped_tbl_xdf <- function(.data, ..., .dots)
{
    dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    needs_mutate <- vapply(exprs, function(e) !is.name(e), logical(1))
    if(any(needs_mutate))
        .data <- mutate_(.data, .dots=exprs[needs_mutate])

    outData <- tblSource(.data)
    xdflst <- split_groups(.data, outData)
    xdflst <- rxExec(distinct_base, data=rxElemArg(xdflst), names(exprs), rxArgs, packagesToLoad="dplyrXdf")
    combine_groups(xdflst, outData, groups(.data))
}


#' @importFrom RevoPemaR pemaCompute
distinct_base <- function(data, vars, rxArgs)
{
    df <- RevoPemaR::pemaCompute(pemaDistinct(), data=data, varName=vars)
    cl <- quote(rxDataStep(df, tblSource(data), overwrite=TRUE))
    cl[names(rxArgs)] <- rxArgs
    eval(cl)
}


#' @importFrom RevoPemaR setPemaClass
#' @importClassesFrom RevoPemaR PemaBaseClass
pemaDistinct <- RevoPemaR::setPemaClass("PemaDistinct",
    contains="PemaBaseClass",

    fields=list(dfList="list", varNames="character"),

    methods=list(
        initialize=function(varNames="", ...) {
            callSuper(...)
            usingMethods(.pemaMethods)
            dfList <<- vector("list", 0)
            varNames <<- varNames
        },

        processData=function(chunk) {
            dfList <<- c(dfList, list(distinct_(as.data.frame(chunk, stringsAsFactors=FALSE), .dots=varNames)))
            invisible(NULL)
        },

        updateResults=function(pemaDistinctObj) {
            dfList <<- c(dfList, pemaDistinctObj$dfList)
            invisible(NULL)
        },

        processResults=function() {
            distinct_(bind_rows(dfList), .dots=varNames)
        }
))

