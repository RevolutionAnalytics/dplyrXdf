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

