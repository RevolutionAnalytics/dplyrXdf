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
#'
#' @examples
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#' tbl1 <- distinct(mtx)
#' tbl2 <- distinct(mtx, am)
#' tbl3 <- distinct(mtx, am, vs)
#' nrow(tbl1)
#' nrow(tbl2)
#' nrow(tbl3)
#'
#' # save to a persistent Xdf file
#' distinct(mtx, am, vs, .outFile="mtcars_distinct.xdf")
#' @aliases distinct distinct_
#' @rdname distinct
#' @export
distinct.RxFileData <- function(.data, ..., .keep_all=FALSE, .outFile=tbl_xdf(.data), .rxArgs)
{
    stopIfDistribCC("distinct not supported in Hadoop/Spark compute context")
    args <- rlang::quos(...)
    .rxArgs <- if(missing(.rxArgs))
        NULL
    else rlang::enexpr(.rxArgs)

    distinctBase(.data, args, .keep_all, .outFile, rlang::enexpr(.rxArgs))
}


#' @details
#' This verb calls \code{dplyr::distinct} on each chunk in an Xdf file. The individual data frames are \code{rbind}ed together and \code{dplyr::distinct} is called on the overall result. This may be slow if there are many chunks in the file; and the operation will be limited by memory if the number of distinct rows is large.
#'
#' This verb can be used on HDFS data in the local compute context (on the edge node), but not in the Hadoop or Spark compute contexts.
#'
#' @return
#' An object representing the unique rows. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @rdname distinct
#' @export
distinct.grouped_tbl_xdf <- function(.data, ..., .keep_all=FALSE, .outFile=tbl_xdf(.data), .rxArgs)
{
    stopIfDistribCC("distinct not supported in Hadoop/Spark compute context")
    args <- rlang::quos(...)
    .rxArgs <- if(missing(.rxArgs))
        NULL
    else rlang::enexpr(.rxArgs)

    grps <- group_vars(.data)

    callGroupedExec(.data, .outFile, distinctBase,
        vars=args, keep_all=.keep_all, outFile=NULL, rxArgs=.rxArgs, grps=grps) %>%
        simpleRegroup(grps)
}


distinctBase <- function(.data, vars, keep_all, outFile, rxArgs, grps=NULL, ...)
{
    require(dplyr)
    output <- rxDataStep(.data, transformFunc=function(varlst)
        {
            if(!.rxIsTestChunk)
            {
                varlst <- as.data.frame(varlst, stringsAsFactors=FALSE)
                df <- dplyr::distinct(varlst, !!!.vars, .keep_all=.keep_all)
                if(length(.grps) > 0 && !keep_all)
                {
                    dfnames <- names(df)
                    # use cbind instead of bind_cols because it recycles rows; need to shut it up about row names
                    df <- suppressWarnings(cbind(varlst[1, .grps[!(.grps %in% dfnames)], drop=FALSE], df))
                }
                .out <<- c(.out, list(df))
            }
            NULL
        },
        transformObjects=list(.vars=vars, .keep_all=keep_all, .grps=grps, .out=list()),
        returnTransformObjects=TRUE)$.out %>%
        bind_rows %>%
        distinct

    if(is.character(outFile) || is_xdf(outFile) || !is.null(rxArgs))
    {
        # explicit namespace reference to allow for parallel/execBy backends: requires dplyrXdf to be installed on nodes
        arglst <- dplyrXdf:::doExtraArgs(list(output), .data, rxArgs, outFile)
        output <- dplyrXdf:::callRx("rxDataStep", arglst)
    }
    output
}

