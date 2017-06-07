#' Join two data sources together
#'
#' @param x, y Data sources to join.
#' @param by Character vector of variables to join by. See \code{\link[dplyr]{join}} for details.
#' @param copy If the data sources are not stored in the same system, whether to copy y to x's location. Not currently used.
#' @param .outFile Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param ... Not currently used.
#'
#' @details
#' Currently joining only supports the local file system.
#'
#' @return
#' An object representing the joined data. This depends on the \code{.outFile} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[dplyr]{join}} in package dplyr
#' @aliases join left_join right_join inner_join full_join semi_join anti_join
#' @name join
NULL

#' @rdname join
#' @export
left_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, suffix=c(".x", ".y"), .outFile, .rxArgs, ...)
{    
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")
    by <- commonBy(by, x, y)
    mergeBase(x, y, by, copy, "left", .outFile, rlang::enexpr(.rxArgs), suffix, ...)
}


#' @rdname join
#' @export
right_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, suffix=c(".x", ".y"), .outFile, .rxArgs, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")
    by <- commonBy(by, x, y)
    mergeBase(x, y, by, copy, "right", .outFile, rlang::enexpr(.rxArgs), suffix, ...)
}


#' @rdname join
#' @export
inner_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, suffix=c(".x", ".y"), .outFile, .rxArgs, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")
    by <- commonBy(by, x, y)
    mergeBase(x, y, by, copy, "inner", .outFile, rlang::enexpr(.rxArgs), suffix, ...)
}


#' @rdname join
#' @export
full_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, suffix=c(".x", ".y"), .outFile, .rxArgs, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")
    by <- commonBy(by, x, y)
    mergeBase(x, y, by, copy, "full", .outFile, rlang::enexpr(.rxArgs), suffix, ...)
}


#' @rdname join
#' @export
semi_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, .outFile, .rxArgs, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")

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
anti_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, .outFile, .rxArgs, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")

    # no native anti-join functionality in ScaleR, so do it by hand
    by <- commonBy(by, x, y)

    # make sure we don't orphan y
    if(inherits(y, "tbl_xdf"))
        y@hasTblFile <- FALSE
    #ones <- sprintf("rep(1L, length(%s))", by$x[1])

    y <- transmute(y, !!!rlang::syms(by$y), .ones=1L) %>% distinct

    on.exit(deleteIfTbl(y))
    mergeBase(x, y, by, copy, "left", .outFile, rlang::enexpr(.rxArgs)) %>%
        subset(is.na(.ones), -.ones)
}


mergeBase <- function(x, y, by=NULL, copy=FALSE, type, .outFile, .rxArgs, suffix=c(".x", ".y"))
{
    stopIfHdfs(x, "merging not supported on HDFS")
    stopIfHdfs(y, "merging not supported on HDFS")

    # copy not used by dplyrXdf at the moment
    if(copy)
        warning("copy argument not currently used")

    grps <- group_vars(x)
    yOrig <- getTblFile(y)
    newxy <- alignInputs(x, y, by, yOrig)
    x <- newxy$x
    y <- newxy$y

    # cleanup on exit is asymmetric wrt x, y
    on.exit({
        deleteIfTbl(x)
        # make sure not to delete original y by accident after factoring
        if(!is.data.frame(y) && getTblFile(y) != yOrig)
            deleteTbl(y)
     })

    # sigh
    if(all(substr(suffix, 1, 1) == "."))
        suffix <- substr(suffix, 2, nchar(suffix))

    arglst <- list(x, y, matchVars=by$y, type=type, duplicateVarExt=suffix)
    arglst <- doExtraArgs(arglst, x, .rxArgs, .outFile)
    arglst$rowsPerRead <- NULL # not used by rxMerge

    output <- rlang::invoke("rxMerge", arglst, .env=parent.frame())
    simpleRegroup(output, grps)
}

