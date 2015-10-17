#' @export
left_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, ...)
{
    left_join(tbl(x), y, by, copy=copy, ...)
}


#' @export
right_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, ...)
{
    right_join(tbl(x), y, by, copy=copy, ...)
}


#' @export
inner_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, ...)
{
    inner_join(tbl(x), y, by, copy=copy, ...)
}


#' @export
full_join.RxFileData <- function(x, y, by=NULL, copy=FALSE, ...)
{
    full_join(tbl(x), y, by, copy=copy, ...)
}


#' @export
left_join.tbl_xdf <- function(x, y, by=NULL, copy=FALSE, ...)
{    
    by <- dplyr_common_by(by, x, y)
    merge_base(x, y, by, copy, "left")
}


#' @export
right_join.tbl_xdf <- function(x, y, by=NULL, copy=FALSE, ...)
{
    by <- dplyr_common_by(by, x, y)
    merge_base(x, y, by, copy, "right")
}


#' @export
inner_join.tbl_xdf <- function(x, y, by=NULL, copy=FALSE, ...)
{
    by <- dplyr_common_by(by, x, y)
    merge_base(x, y, by, copy, "inner")
}


#' @export
full_join.tbl_xdf <- function(x, y, by=NULL, copy=FALSE, ...)
{
    by <- dplyr_common_by(by, x, y)
    merge_base(x, y, by, copy, "full")
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
semi_join.RxFileData <- function(x, y, ...)
{
    stop("semi joins not supported for Rx data sources", call.=FALSE)
}


#' @rdname setops 
#' @export
anti_join.RxFileData <- function(x, y, ...)
{
    stop("anti joins not supported for Rx data sources", call.=FALSE)
}


#' @rdname setops 
#' @export
intersect.RxFileData <- function(x, y, ...)
{
    stop("intersect not supported for Rx data sources", call.=FALSE)
}


#' @rdname setops 
#' @export
union.RxFileData <- function(x, y, ...)
{
    stop("union not supported for Rx data sources", call.=FALSE)
}


#' @rdname setops 
#' @export
setdiff.RxFileData <- function(x, y, ...)
{
    stop("setdiff not supported for Rx data sources", call.=FALSE)
}


merge_base <- function(x, y, by=NULL, copy=FALSE, type)
{
    if(!hasTblFile(y))
    {
        on.exit({
            ytbl <- tblFile(y)
            if(file.exists(ytbl)) file.remove(ytbl)
        })
    }
    newxy <- xdf_copy(x, y, copy, by)
    x <- newxy$x
    y <- newxy$y
    out <- rxMerge(x, y, matchVars=by$x, outFile=tblFile(x), type=type, duplicateVarExt=c("x", "y"), overwrite=TRUE)
    tblFile(x) <- rxGetInfo(out)$fileName
    x
}


# copied from dplyr:::common_by, dplyr:::`%||%`
dplyr_common_by <- function (by = NULL, x, y) 
{
    if (is.list(by)) 
        return(by)
    if (!is.null(by)) {
        x <- if(is.null(names(by))) by else names(by)
        #x <- names(by) %||% by
        y <- unname(by)
        x[x == ""] <- y[x == ""]
        return(list(x = x, y = y))
    }
    by <- intersect(tbl_vars(x), tbl_vars(y))
    if (length(by) == 0) {
        stop("No common variables. Please specify `by` param.", call. = FALSE)
    }
    message("Joining by: ", capture.output(dput(by)))
    list(x = by, y = by)
}
