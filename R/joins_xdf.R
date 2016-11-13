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


# duplicate the generic from dplyr 0.5, to allow install with dplyr <= 0.4.3
#' @export
union_all <- function(x, y, ...)
UseMethod("union_all")


#' @export
union_all.RxFileData <- function(x, y, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")

    # use x's tbl if it exists; otherwise create a new tbl, copy x into it
    xTbl <- if(hasTblFile(x))
        tblSource(x)
    else rxDataStep(x, newTbl(x))

    # if y points to same file as xTbl, make a copy
    # should only happen with union_all(x, x) with x a tbl
    if(inherits(y, "RxFileData") && y@file == xTbl@file)
    {
        y <- rxDataStep(y, newTbl(y))
        on.exit(file.remove(y@file))
    }

    # append y to x
    tbl(rxDataStep(y, xTbl, append="rows", ...), hasTblFile=TRUE)
}


#' @export
union.RxFileData <- function(x, y, ...)
{
    stopIfHdfs(x, "joining not supported on HDFS")
    stopIfHdfs(y, "joining not supported on HDFS")

    # call union_all.RxFileData explicitly to allow use in dplyr < 0.5
    union_all.RxFileData(x, y, ...) %>% distinct
}


#' Unsupported joins/set operations
#'
#' These operations are currently not supported for ScaleR data sources.
#'
#' @param x, y ScaleR data sources, or tbls wrapping the same.
#' @param ... Other arguments passed on to methods.
#'
#' @rdname setops 
#' @export
intersect.RxFileData <- function(x, y, ...)
{
    stop("intersect not supported for Rx data sources", call.=FALSE)
}


#' @rdname setops 
#' @export
setdiff.RxFileData <- function(x, y, ...)
{
    stop("setdiff not supported for Rx data sources", call.=FALSE)
}


#' @rdname setops 
#' @export
setequal.RxFileData <- function(x, y, ...)
{
    stop("setequal not supported for Rx data sources", call.=FALSE)
}


merge_base <- function(x, y, by=NULL, copy=FALSE, type, .output=NA, .rxArgs=NULL)
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
    .output <- createOutput(x, .output)
    cl <- quote(rxMerge(x, y, matchVars=by$y, outFile=.output, type=type))
    cl[names(.rxArgs)] <- .rxArgs

    out <- eval(cl)
    simpleRegroup(out)
}

