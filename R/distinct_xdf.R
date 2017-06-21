#' Select distinct/unique rows
#'
#' @param .data A tbl for an Xdf data source; or a raw Xdf data source.
#' @param ... Variables to use for determining uniqueness. If left blank, all variables in \code{.data} are used to determine uniqueness.
#' @param .keep_all Whether to keep all the variables in the dataset, or only those used in determining uniqueness.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param .keep_all If \code{TRUE}, keep all variables in the dataset; otherwise, only keep variables used in defining uniqueness.
#'
#' @seealso
#' \code{\link[dplyr]{distinct}} in package dplyr
#' @aliases distinct distinct_
#' @rdname distinct
#' @export
distinct.RxFileData <- function(.data, ..., .keep_all=FALSE, .outFile=tbl_xdf(.data), .rxArgs)
{
    stopIfHdfs(.data, "distinct not supported on HDFS")
    dots <- rlang::quos(...)

    distinctBase(.data, dots, .keep_all, .outFile, rlang::enexpr(.rxArgs))
}


#' @details
#' This verb calls \code{dplyr::distinct} on each chunk in an Xdf file. The individual data frames are \code{rbind}ed together and \code{dplyr::distinct} is called on the overall result. This may be slow if there are many chunks in the file; and the operation will be limited by memory if the number of distinct rows is large.
#'
#' @return
#' An object representing the unique rows. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @rdname distinct
#' @export
distinct.grouped_tbl_xdf <- function(.data, ..., .keep_all=FALSE, .outFile=tbl_xdf(.data), .rxArgs)
{
    stopIfHdfs(.data, "distinct not supported on HDFS")

    dots <- rlang::quos(...)
    grps <- group_vars(.data)

    # workaround to keep rxExec from complaining about missing arg
    .rxArgs <- if(missing(.rxArgs))
        NULL
    else rlang::enexpr(.rxArgs)

    outlst <- if(.dxOptions$useExecBy)
        distinctExecBy(.data, dots, .keep_all, .rxArgs, grps)
    else
    {
        xdflst <- splitGroups(.data)
        on.exit(deleteIfTbl(xdflst))
        outlst <- createSplitOutput(xdflst, .outFile)

        rxExec(distinctBase, data=rxElemArg(xdflst), dots, .keep_all, rxElemArg(outlst), .rxArgs, grps,
            execObjects=c("distinctBase2", "doExtraArgs", "deleteIfTbl", "deleteTbl", ".dxOptions"),
            packagesToLoad="dplyr")
    }
    combineGroups(outlst, .outFile, grps)
}


distinctBase <- function(data, vars, keep_all, output, rxArgs, grps=NULL)
{
    data <- distinctBase2(data, vars, keep_all, grps)
    arglst <- doExtraArgs(list(data), data, rxArgs, output)

    if(missing(output) || inherits(output, "RxXdfData") || !missing(rxArgs))
        rlang::invoke("rxDataStep", arglst, .env=parent.frame(), .bury=NULL)
    else data
}


# need to define separate function for transform because missing arguments to distinctBase will break rxDataStep
distinctBase2 <- function(data, vars, keep_all, grps=NULL)
{
    data <- rxDataStep(data, transformFunc=function(varlst)
        {
            varlst <- data.frame(varlst)
            df <- distinct(varlst, !!!.vars, .keep_all=.keep_all)
            if(length(.grps) > 0 && !keep_all)
                df <- cbind(varlst[1, .grps], df)
            if(!.rxIsTestChunk)
                .out <<- c(.out, list(df))
            NULL
        },
        transformObjects=list(.vars=vars, .keep_all=keep_all, .grps=grps, .out=list()),
        transformPackages="dplyr",
        returnTransformObjects=TRUE)$.out %>%
        bind_rows %>%
        distinct
    if(length(grps) > 0 && !keep_all)
    {
        ngrps <- length(grps)
        names(data)[ngrps] <- grps
    }
    data
}


distinctExecBy <- function(data, vars, keep_all, rxArgs, grps=NULL)
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    dflst <- execByResult(rxExecBy(data, grps, function(keys, data, keep_all)
        {
            require(dplyr)
            data <- rxDataStep(data, transformFunc=function(varlst)
                {
                    varlst <- data.frame(varlst)
                    df <- distinct(varlst, !!!.vars, .keep_all=.keep_all)
                    if(length(.grps) > 0 && !keep_all)
                        df <- cbind(varlst[1, .grps], df)
                    if(!.rxIsTestChunk)
                        .out <<- c(.out, list(df))
                    NULL
                },
                transformObjects=list(.vars=vars, .keep_all=keep_all, .grps=grps, .out=list()),
                transformPackages="dplyr",
                returnTransformObjects=TRUE)$.out %>%
                bind_rows %>%
                distinct
            if(length(grps) > 0 && !keep_all)
            {
                ngrps <- length(grps)
                names(data)[ngrps] <- grps
            }
            data
        },
        list(keep_all=keep_all)))

    # run rxDataStep if rxArgs specified
    if(!is.null(rxArgs))
        lapply(dflst, function(df)
        {
            arglst <- doExtraArgs(list(df), df, rxArgs, NULL)
            rlang::invoke("rxDataStep", arglst, .env=parent.frame(), .bury=NULL)
        })
    else dflst
}


