#' Select distinct/unique rows
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param ... Variables to use for determining uniqueness. If left blank, all variables in \code{.data} are used to determine uniqueness.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#' @param .keep_all If \code{TRUE}, keep all variables in the dataset; otherwise, only keep variables used in defining uniqueness. Only used if dplyr version 0.5 or greater is installed.
#'
#' @seealso
#' \code{\link[dplyr]{distinct}} in package dplyr
#' @aliases distinct
#' @rdname distinct
#' @export
distinct_.RxFileData <- function(.data, ..., .dots, .keep_all=FALSE)
{
    stopIfHdfs(.data, "distinct not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    # hack to accommodate dplyr 0.5 keep_all argument when dplyr 0.4.x is installed
    if(!is.null(exprs$.keep_all))
    {
        .keep_all <- exprs$.keep_all
        exprs$.keep_all <- NULL
    }

    needs_mutate <- vapply(exprs, function(e) !is.name(e), logical(1))
    if(any(needs_mutate))
        .data <- mutate_(.data, .dots=exprs[needs_mutate])

    tbl(distinct_base(.data, names(exprs), rxArgs, keep_all=.keep_all), file=NULL, hasTblFile=TRUE)
}


#' @details
#' To process a grouped Xdf tbl, \code{distinct} splits the data into one file per group, and calls \code{\link[dplyr]{distinct}} on each. The individual data frames are \code{rbind}ed together and \code{distinct} is called on the overall result. This ensures that the groups will be appropriately generated regardless of the types of the grouping variables. Note however that this may be slow if you have a large number of groups; and the operation will be limited by memory if the number of distinct rows is large.
#' @rdname distinct
#' @export
distinct_.grouped_tbl_xdf <- function(.data, ..., .dots, .keep_all=FALSE)
{
    stopIfHdfs(x, "distinct not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)

    # identify Revo-specific arguments
    dots <- .rxArgs(dots)
    rxArgs <- dots$rxArgs
    exprs <- dots$exprs

    # hack to accommodate dplyr 0.5 keep_all argument when dplyr 0.4.x is installed
    if(!is.null(exprs$.keep_all))
    {
        .keep_all <- exprs$.keep_all
        exprs$.keep_all <- NULL
    }

    needs_mutate <- vapply(exprs, function(e) !is.name(e), logical(1))
    if(any(needs_mutate))
        .data <- mutate_(.data, .dots=exprs[needs_mutate])

    outData <- tblSource(.data)
    xdflst <- split_groups(.data, outData)
    xdflst <- rxExec(distinct_base, data=rxElemArg(xdflst), names(exprs), rxArgs, keep_all=.keep_all,
        tblDir=tempdir(), execObjects=c("pemaDistinct", "newTbl"), packagesToLoad="dplyrXdf")
    combine_groups(xdflst, outData, groups(.data))
}


#' @importFrom RevoPemaR pemaCompute
distinct_base <- function(data, vars, rxArgs, keep_all, tblDir=tempdir())
{
    df <- RevoPemaR::pemaCompute(pemaDistinct(), data=data, varNames=vars, keep_all=keep_all)
    cl <- quote(rxDataStep(df, newTbl(data, tblDir=tblDir), overwrite=TRUE))
    cl[names(rxArgs)] <- rxArgs
    eval(cl)
}


#' @importFrom RevoPemaR setPemaClass
#' @importClassesFrom RevoPemaR PemaBaseClass
pemaDistinct <- RevoPemaR::setPemaClass("PemaDistinct",
    contains="PemaBaseClass",

    fields=list(dfList="list", varNames="character", keep_all="logical", dplyr5="logical"),

    methods=list(
        initialize=function(varNames="", keep_all=logical(0), ...) {
            callSuper(...)
            usingMethods(.pemaMethods)
            dfList <<- vector("list", 0)
            varNames <<- varNames
            # keep_all option only for dplyr versions >= 0.5
            keep_all <<- keep_all
            dplyr5 <<- .dxOptions$dplyrVersion >= package_version("0.5")
        },

        processData=function(chunk) {
            # keep_all option only for dplyr versions >= 0.5
            if(dplyr5 && !keep_all && length(varNames) > 0)
                chunk <- chunk[varNames]
            dfChunk <- as.data.frame(chunk, stringsAsFactors=FALSE)
            dfList <<- if(dplyr5)
                c(dfList, list(distinct_(dfChunk, .dots=varNames, .keep_all=keep_all)))
            else c(dfList, list(distinct_(dfChunk, .dots=varNames)))
            invisible(NULL)
        },

        updateResults=function(pemaDistinctObj) {
            dfList <<- c(dfList, pemaDistinctObj$dfList)
            invisible(NULL)
        },

        processResults=function() {
            # keep_all option only for dplyr versions >= 0.5
            if(dplyr5)
                distinct_(bind_rows(dfList), .dots=varNames, .keep_all=keep_all)
            else distinct_(bind_rows(dfList), .dots=varNames)
        }
))

