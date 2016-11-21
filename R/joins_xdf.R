#' Join two data sources together
#'
#' @param x, y Data sources to join.
#' @param by Character vector of variables to join by. See \code{\link[dplyr]{join}} for details.
#' @param copy If the data sources are not stored in the same system, whether to copy y to x's location. Not currently used.
#' @param .output Output format for the returned data. If not supplied, create an xdf tbl; if \code{NULL}, return a data frame; if a character string naming a file, save an Xdf file at that location.
#' @param .rxArgs A list of RevoScaleR arguments. See \code{\link{rxArgs}} for details.
#' @param ... Not currently used.
#'
#' @details
#' Currently joining only supports the local file system.
#'
#' @return
#' An object representing the joined data. This depends on the \code{.output} argument: if missing, it will be an xdf tbl object; if \code{NULL}, a data frame; and if a filename, an Xdf data source referencing a file saved to that location.
#'
#' @seealso
#' \code{\link[dplyr]{join}} in package dplyr
#' @aliases join left_join right_join inner_join full_join semi_join anti_join
#' @name join
NULL

#' @rdname join
#' @export
left_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, .output, .rxArgs, ...)
{    
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")
    by <- dplyr_common_by(by, x, y)
    merge_base(x, y, by, copy, "left", .output, .rxArgs, ...)
}


#' @rdname join
#' @export
right_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, .output, .rxArgs, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")
    by <- dplyr_common_by(by, x, y)
    merge_base(x, y, by, copy, "right", .output, .rxArgs, ...)
}


#' @rdname join
#' @export
inner_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, .output, .rxArgs, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")
    by <- dplyr_common_by(by, x, y)
    merge_base(x, y, by, copy, "inner", .output, .rxArgs, ...)
}


#' @rdname join
#' @export
full_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, .output, .rxArgs, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")
    by <- dplyr_common_by(by, x, y)
    merge_base(x, y, by, copy, "full", .output, .rxArgs, ...)
}


#' @rdname join
#' @export
semi_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, .output, .rxArgs, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")

    # no native semi-join functionality in ScaleR, so do it by hand
    by <- dplyr_common_by(by, x, y)

    # make sure we don't orphan y
    if(inherits(y, "tbl_xdf"))
        y@hasTblFile <- FALSE
    y <- select_(y, by$y) %>% distinct
    on.exit({
        yFile <- tblSource(y)
        deleteTbl(yFile)
    })
    merge_base(x, y, by, copy, "inner", .output, .rxArgs,  ...)
}


#' @rdname join
#' @export
anti_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, .output, .rxArgs, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")

    # no native anti-join functionality in ScaleR, so do it by hand
    by <- dplyr_common_by(by, x, y)

    # make sure we don't orphan y
    if(inherits(y, "tbl_xdf"))
        y@hasTblFile <- FALSE
    ones <- sprintf("rep(1L, length(%s))", by$x[1])
    y <- transmute_(y, by$y, .ones=ones) %>% distinct
    on.exit({
        yFile <- tblSource(y)
        deleteTbl(yFile)
    })
    merge_base(x, y, by, copy, "left", .output, .rxArgs, ...) %>%
        subset(is.na(.ones), -.ones)
}


merge_base <- function(x, y, by=NULL, copy=FALSE, type, .output, .rxArgs)
{
    stopIfHdfs(x, "merging not supported on HDFS")
    stopIfHdfs(y, "merging not supported on HDFS")

    # copy not used by dplyrXdf at the moment
    if(copy)
        warning("copy argument not currently used")

    yOrig <- getTblFile(y)
    newxy <- alignInputs(x, y, by, yOrig)
    x <- newxy$x
    y <- newxy$y

    # cleanup on exit is asymmetric wrt x, y
    on.exit({
        if(hasTblFile(x))
            deleteTbl(x)
        # make sure not to delete original y by accident after factoring
        if(!is.data.frame(y) && getTblFile(y) != yOrig)
            deleteTbl(y)
     })

    if(missing(.output))
        .output <- NA
    if(missing(.rxArgs))
        .rxArgs <- NULL

    grps <- groups(x)
    .output <- createOutput(x, .output)
    cl <- quote(rxMerge(x, y, matchVars=by$y, outFile=.output, type=type, duplicateVarExt=c("x", "y"), overwrite=TRUE))
    cl[names(.rxArgs)] <- .rxArgs

    out <- eval(cl)
    simpleRegroup(out)
}

