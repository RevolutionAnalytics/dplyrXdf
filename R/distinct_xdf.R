#' Select distinct/unique rows
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param ... Variables to use for determining uniqueness. If left blank, all variables in \code{.data} are used to determine uniqueness.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .dots Used to work around non-standard evaluation. See the dplyr vignettes for details.
#' @param .keep_all If \code{TRUE}, keep all variables in the dataset; otherwise, only keep variables used in defining uniqueness. Only used if dplyr version 0.5 or greater is installed.
#'
#' @seealso
#' \code{\link[dplyr]{distinct}} in package dplyr
#' @aliases distinct distinct_
#' @rdname distinct
#' @export
distinct_.RxFileData <- function(.data, ..., .outFile, .rxArgs, .dots, .keep_all=FALSE)
{
    stopIfHdfs(.data, "distinct not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ..., all_named=TRUE)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.outFile)) .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs

    # hack to accommodate dplyr 0.5 keep_all argument when dplyr 0.4.x is installed
    if(!is.null(exprs$.keep_all))
    {
        .keep_all <- exprs$.keep_all
        exprs$.keep_all <- NULL
    }

    needs_mutate <- vapply(exprs, function(e) !is.name(e), logical(1))
    if(any(needs_mutate))
        .data <- mutate_(.data, .dots=exprs[needs_mutate])

    .outFile <- createOutput(.data, .outFile)
    .data <- distinct_base(.data, .outFile, names(exprs), .rxArgs, keep_all=.keep_all)
    simpleRegroup(.data)
}


#' @details
#' This verb calls \code{dplyr::distinct} on each chunk in an Xdf file. The individual data frames are \code{rbind}ed together and \code{dplyr::distinct} is called on the overall result. This may be slow if there are many chunks in the file; and the operation will be limited by memory if the number of distinct rows is large.
#'
#' @return
#' An object representing the unique rows. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @rdname distinct
#' @export
distinct_.grouped_tbl_xdf <- function(.data, ..., .outFile, .rxArgs, .dots, .keep_all=FALSE)
{
    stopIfHdfs(.data, "distinct not supported on HDFS")

    dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)

    # .outFile and .rxArgs will be passed in via .dots if called by NSE
    dots <- rxArgs(dots)
    exprs <- dots$exprs
    if(missing(.outFile)) .outFile <- dots$output
    if(missing(.rxArgs)) .rxArgs <- dots$rxArgs
    grps <- groups(.data)

    # hack to accommodate dplyr 0.5 keep_all argument when dplyr 0.4.x is installed
    if(!is.null(exprs$.keep_all))
    {
        .keep_all <- exprs$.keep_all
        exprs$.keep_all <- NULL
    }

    needs_mutate <- vapply(exprs, function(e) !is.name(e), logical(1))
    if(any(needs_mutate))
        .data <- mutate_(.data, .dots=exprs[needs_mutate])

    xdflst <- split_groups(.data)
    outlst <- createSplitOutput(xdflst, .outFile)
    outlst <- rxExec(distinct_base, data=rxElemArg(xdflst), output=rxElemArg(outlst), names(exprs), .rxArgs, .keep_all,
        execObjects=c("pemaDistinct", "deleteTbl"), packagesToLoad="dplyrXdf")
    combine_groups(outlst, createOutput(.data, .outFile), grps)
}


#' @importFrom RevoPemaR pemaCompute
distinct_base <- function(data, output, vars, rxArgs, keep_all)
{
    oldData <- data
    if(hasTblFile(data))
        on.exit(deleteTbl(oldData))

    dplyr5 <- .dxOptions$dplyrVersion >= package_version("0.5")
    df <- rxDataStep(data, transformFunc=function(varlst) {
            df <- if(dplyr5)
                dplyr::distinct_(data.frame(varlst), .dots=.vars, .keep_all=.keep_all)
            else dplyr::distinct_(data.frame(varlst), .dots=.vars)
            if(!.rxIsTestChunk)
                .out <<- c(.out, list(df))
            NULL
        },
        transformObjects=list(.vars=vars, .keep_all=keep_all, .dplyr5=dplyr5, .out=list()),
        transformPackages="dplyr",
        returnTransformObjects=TRUE)$.out %>% dplyr::bind_rows(.)

    df <- if(dplyr5)
        dplyr::distinct_(df, .dots=vars, .keep_all=keep_all)
    else dplyr::distinct_(df, .dots=vars)
    
    if(inherits(output, "RxXdfData"))
    {
        cl <- quote(rxDataStep(df, output, overwrite=TRUE, rowsPerRead=.dxOptions$rowsPerRead))
        cl[names(rxArgs)] <- rxArgs
        eval(cl)
    }
    else df
}


##' @importFrom RevoPemaR setPemaClass
##' @importClassesFrom RevoPemaR PemaBaseClass
#pemaDistinct <- RevoPemaR::setPemaClass("PemaDistinct",
    #contains="PemaBaseClass",

    #fields=list(dfList="list", varNames="character", keep_all="logical", dplyr5="logical"),

    #methods=list(
        #initialize=function(varNames="", keep_all=logical(0), ...) {
            #callSuper(...)
            #usingMethods(.pemaMethods)
            #dfList <<- vector("list", 0)
            #varNames <<- varNames
            ## keep_all option only for dplyr versions >= 0.5
            #keep_all <<- keep_all
            #dplyr5 <<- .dxOptions$dplyrVersion >= package_version("0.5")
        #},

        #processData=function(chunk) {
            ## keep_all option only for dplyr versions >= 0.5
            #if(dplyr5 && !keep_all && length(varNames) > 0)
                #chunk <- chunk[varNames]
            #dfChunk <- as.data.frame(chunk, stringsAsFactors=FALSE)
            #dfList <<- if(dplyr5)
                #c(dfList, list(distinct_(dfChunk, .dots=varNames, .keep_all=keep_all)))
            #else c(dfList, list(distinct_(dfChunk, .dots=varNames)))
            #invisible(NULL)
        #},

        #updateResults=function(pemaDistinctObj) {
            #dfList <<- c(dfList, pemaDistinctObj$dfList)
            #invisible(NULL)
        #},

        #processResults=function() {
            ## keep_all option only for dplyr versions >= 0.5
            #if(dplyr5)
                #distinct_(bind_rows(dfList), .dots=varNames, .keep_all=keep_all)
            #else distinct_(bind_rows(dfList), .dots=varNames)
        #}
#))

