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

    args <- rlang::quos(...)
    grps <- group_vars(.data)

    # workaround to keep rxExec from complaining about missing arg
    .rxArgs <- if(missing(.rxArgs))
        NULL
    else rlang::enexpr(.rxArgs)

    callFunc <- if(.dxOptions$useExecBy) callExecBy else callSplit

    callFunc(.data, distinctBase, vars=args, keep_all=.keep_all, output=NULL, rxArgs=.rxArgs, grps=grps) %>%
        combineGroups(.outFile, grps)
}


distinctBase <- function(.data, vars, keep_all, output, rxArgs, grps=NULL, ...)
{
    require(dplyr)
    varNames <- select_vars(names(.data), !!!vars)

    .data <- rxDataStep(.data, transformFunc=function(varlst)
        {
            if(!.rxIsTestChunk)
            {
                varlst <- dplyr::as_data_frame(varlst)
                df <- dplyr::distinct(varlst, !!!.vars, .keep_all=.keep_all)
                if(length(.grps) > 0 && !keep_all)
                    df <- cbind(varlst[1, .grps], df)
                .out <<- c(.out, list(df))
            }
            NULL
        },
        transformObjects=list(.vars=vars, .keep_all=keep_all, .grps=grps, .out=list()),
        returnTransformObjects=TRUE)$.out %>%
        bind_rows %>%
        distinct
    # put grouping columns back on, if not already there
    if(length(grps) > 0 && !keep_all)
    {
        grpsToDrop <- grps %in% varNames
        grpsToKeep <- which(!grpsToDrop)
        names(.data)[grpsToKeep] <- grps[grpsToKeep]
        if(any(grpsToDrop))
            .data <- .data[-which(grpsToDrop)]
    }

    if(missing(output) || inherits(output, "RxXdfData") || !missing(rxArgs))
    {
        # explicit namespace reference to allow for parallel/execBy backends
        arglst <- dplyrXdf:::doExtraArgs(list(.data), .data, rxArgs, output)
        .data <- rlang::invoke("rxDataStep", arglst, .env=parent.frame(), .bury=NULL)
    }
    .data
}

