#' Select distinct/unique rows
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param ... Variables to use for determining uniqueness. If left blank, all variables in \code{.data} are used to determine uniqueness.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .keep_all If \code{TRUE}, keep all variables in the dataset; otherwise, only keep variables used in defining uniqueness.
#'
#' @seealso
#' \code{\link[dplyr]{distinct}} in package dplyr
#' @aliases distinct distinct_
#' @rdname distinct
#' @export
distinct.RxFileData <- function(.data, ..., .keep_all=FALSE, .outFile, .rxArgs)
{
    stopIfHdfs(.data, "distinct not supported on HDFS")
    dots <- rlang::quos(...)

    .data <- distinctBase(.data, dots, .keep_all, .outFile, rlang::enexpr(.rxArgs))
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
distinct.grouped_tbl_xdf <- function(.data, ..., .keep_all=FALSE, .outFile, .rxArgs)
{
    stopIfHdfs(.data, "distinct not supported on HDFS")

    dots <- rlang::quos(...)

    grps <- group_vars(.data)

    xdflst <- splitGroups(.data)
    outlst <- createSplitOutput(xdflst, .outFile)
    outlst <- rxExec(distinctBase, data=rxElemArg(xdflst), dots, .keep_all, rxElemArg(outlst), rlang::enexpr(.rxArgs),
        execObjects="deleteIfTbl", packagesToLoad="dplyrXdf")
    combineGroups(outlst, .outFile, grps)
}


distinctBase <- function(data, vars, keep_all, output, rxArgs)
{
    oldData <- data

    data <- distinctBase2(data, vars, keep_all) %>%
        #rxDataStep(data, transformFunc=function(varlst) {
                #df <- distinct(data.frame(varlst), !!!.vars, .keep_all=.keep_all)
                #if(!.rxIsTestChunk)
                    #.out <<- c(.out, list(df))
                #NULL
            #},
            #transformObjects=list(.vars=vars, .keep_all=keep_all, .out=list()),
            #transformPackages="dplyr",
            #returnTransformObjects=TRUE)$.out %>%
        bind_rows %>%
        distinct(!!!vars, .keep_all=keep_all)

    arglst <- doExtraArgs(list(data), data, rxArgs, output)
    on.exit(deleteIfTbl(oldData))
    if(missing(output) || inherits(output, "RxXdfData") || !missing(rxArgs))
        rlang::invoke("rxDataStep", arglst, .env=parent.frame())
    else data
}


# need to define separate function for transform because missing arguments to distinctBase will break rxDataStep
distinctBase2 <- function(data, vars, keep_all)
{
    data <- rxDataStep(data, transformFunc=function(varlst)
        {
            df <- distinct(data.frame(varlst), !!!.vars, .keep_all=.keep_all)
            if(!.rxIsTestChunk)
                .out <<- c(.out, list(df))
            NULL
        },
        transformObjects=list(.vars=vars, .keep_all=keep_all, .out=list()),
        transformPackages="dplyr",
        returnTransformObjects=TRUE)
    data$.out
}

