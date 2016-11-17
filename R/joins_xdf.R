#' @export
left_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, ...)
{    
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")
    by <- dplyr_common_by(by, x, y)
    merge_base(x, y, by, copy, "left", ...)
}


#' @export
right_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")
    by <- dplyr_common_by(by, x, y)
    merge_base(x, y, by, copy, "right", ...)
}


#' @export
inner_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")
    by <- dplyr_common_by(by, x, y)
    merge_base(x, y, by, copy, "inner", ...)
}


#' @export
full_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")
    by <- dplyr_common_by(by, x, y)
    merge_base(x, y, by, copy, "full", ...)
}


#' @export
semi_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, ...)
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
    merge_base(x, y, by, copy, "inner", ...)
}


#' @export
anti_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, ...)
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
    merge_base(x, y, by, copy, "left", ...) %>%
        subset(is.na(.ones), -.ones)
}


merge_base <- function(x, y, by=NULL, copy=FALSE, type, .output, .rxArgs=NULL)
{
    stopIfHdfs(x, "merging not supported on HDFS")
    stopIfHdfs(y, "merging not supported on HDFS")

    # copy not used by dplyrXdf; only for dplyr compatibility
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

    grps <- groups(x)
    is(missing(.output))
        .output <- NA
    .output <- createOutput(x, .output)
    cl <- quote(rxMerge(x, y, matchVars=by$y, outFile=.output, type=type, duplicateVarExt=c("x", "y")))
    cl[names(.rxArgs)] <- .rxArgs

    out <- eval(cl)
    simpleRegroup(out)
}

