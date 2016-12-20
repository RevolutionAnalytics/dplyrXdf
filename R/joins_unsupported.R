#' @importFrom dplyr intersect setdiff setequal
NULL

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

