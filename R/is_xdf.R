#' Detect and coerce to Xdf data source objects
#'
#' Functions to detect and coerce to Xdf data source objects.
#'
#' @param x An R object.
#'
#' @details
#' The \code{is_xdf} function returns TRUE if \code{x} is an Xdf data source object; ie, it inherits from the \code{RxXdfData} class. This includes both raw Xdf data sources and \code{tbl_xdf} objects as created by dplyrXdf. The \code{is_composite_xdf} function returns TRUE if \code{x} is a \emph{composite} Xdf data source, and \code{is_standard_xdf} returns TRUE if \code{x} is an Xdf but not a composite Xdf.
#'
#' Detecting whether an object is a composite Xdf can be tricky and \code{is_composite_xdf} goes through a few steps to do this. If \code{x} has a non-NULL \code{createCompositeSet} slot, then that value is returned. Otherwise, it checks whether the \code{file} slot refers to an existing directory, whose name does \emph{not} have an extension (that is, \code{"foo"} qualifies as a valid filename for a composite Xdf, but not \code{"foo.xdf"}). This is necessary because of the semantics of \code{rxDataStep}.
#'
#' To remove any ambiguity, it's recommended that you always explicitly specify the \code{createCompositeSet} argument when creating an Xdf data source object (objects created by dplyrXdf will always do this).
#' @rdname as_xdf
#' @export
is_xdf <- function(x)
{
    inherits(x, "RxXdfData")
}

#' @rdname as_xdf
#' @export
is_composite_xdf <- function(x)
{
    if(!is_xdf(x))
        return(FALSE)

    composite <- x@createCompositeSet
    if(!is.null(composite))
        return(composite)

    # check if this file refers to an existing directory
    file <- x@file
    if(in_hdfs(x))
        return(tools::file_ext(file) == "" && hdfs_dir_exists(file))
    else return(tools::file_ext(file) == "" && dir.exists(file))
}

#' @rdname as_xdf
#' @export
is_standard_xdf <- function(x)
{
    is_xdf(x) && !is_composite_xdf(x)
}
