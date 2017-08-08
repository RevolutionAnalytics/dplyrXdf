#' Join two data sources together
#'
#' @param x, y Data sources to join.
#' @param by Character vector of variables to join by. See \code{\link[dplyr]{join}} for details.
#' @param copy If the data sources are not stored in the same filesystem, whether to copy y to x's location. Not currently implemented.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param ... Not currently used.
#'
#' @details
#' These functions merge two datasets together, using \code{rxMerge}.
#'
#' For best performance, avoid merging on factor variables or on variables with different types, especially in Spark. This is because \code{rxMerge} is picky about its inputs, and dplyrXdf may have to transform the data to ensure that the merge succeeds.
#'
#' Currently, merging in Spark has a few limitations. Only Xdf (in HDFS) and Spark data sources (\code{RxHiveData}, \code{RxOrcData} and \code{RxParquetData}) can be merged, and only the "standard" join operations are supported: \code{left_join}, \code{right_join}, \code{inner_join} and \code{full join}.
#'
#' @return
#' An object representing the joined data. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[dplyr]{join}} in package dplyr, \code{\link{rxMerge}}
#' @aliases join left_join right_join inner_join full_join semi_join anti_join
#' @name join
NULL

#' @rdname join
#' @export
left_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, suffix=c(".x", ".y"), .outFile=tbl_xdf(x), .rxArgs, ...)
{
    by <- commonBy(by, x, y)
    mergeBase(x, y, by, copy, "left", .outFile, rlang::enexpr(.rxArgs), suffix)
}


#' @rdname join
#' @export
right_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, suffix=c(".x", ".y"), .outFile=tbl_xdf(x), .rxArgs, ...)
{
    by <- commonBy(by, x, y)
    mergeBase(x, y, by, copy, "right", .outFile, rlang::enexpr(.rxArgs), suffix)
}


#' @rdname join
#' @export
inner_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, suffix=c(".x", ".y"), .outFile=tbl_xdf(x), .rxArgs, ...)
{
    by <- commonBy(by, x, y)
    mergeBase(x, y, by, copy, "inner", .outFile, rlang::enexpr(.rxArgs), suffix)
}


#' @rdname join
#' @export
full_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, suffix=c(".x", ".y"), .outFile=tbl_xdf(x), .rxArgs, ...)
{
    by <- commonBy(by, x, y)
    mergeBase(x, y, by, copy, "full", .outFile, rlang::enexpr(.rxArgs), suffix)
}


#' @rdname join
#' @export
semi_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, .outFile=tbl_xdf(x), .rxArgs, ...)
{
    stopIfHdfs(x, "semi_join not supported on HDFS")
    stopIfHdfs(y, "semi_join not supported on HDFS")

    # no native semi-join functionality in ScaleR, so do it by hand
    by <- commonBy(by, x, y)

    # make sure we don't orphan y
    if(inherits(y, "tbl_xdf"))
        y@hasTblFile <- FALSE
    y <- select(y, !!!rlang::syms(by$y)) %>% distinct
    if(inherits(y, "tbl_xdf") && y@hasTblFile)
        on.exit(deleteIfTbl(y))
    mergeBase(x, y, by, copy, "inner", .outFile, rlang::enexpr(.rxArgs))
}


#' @rdname join
#' @export
anti_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, .outFile=tbl_xdf(x), .rxArgs, ...)
{
    stopIfHdfs(x, "anti_join not supported on HDFS")
    stopIfHdfs(y, "anti_join not supported on HDFS")

    # no native anti-join functionality in ScaleR, so do it by hand
    by <- commonBy(by, x, y)

    # make sure we don't orphan y
    if(inherits(y, "tbl_xdf"))
        y@hasTblFile <- FALSE
    #ones <- sprintf("rep(1L, length(%s))", by$x[1])

    y <- transmute(y, !!!rlang::syms(by$y), .ones=1L) %>% distinct

    on.exit(deleteIfTbl(y))
    out <- mergeBase(x, y, by, copy, "left", , rlang::enexpr(.rxArgs)) %>%  # note missing .outFile arg!
        subset(is.na(.ones),  -.ones, .outFile=.outFile)
}

