#' @include tbl_xdf.R
NULL


#' @exportClass grouped_tbl_xdf
#' @export grouped_tbl_xdf
grouped_tbl_xdf <- setClass("grouped_tbl_xdf", contains="tbl_xdf", slots=c(groups="characterORNULL"))


#' Group an Xdf file by one or more variables
#'
#' @param .data An Xdf file or a tbl wrapping the same.
#' @param ... Variables to group by.
#' @param add If FALSE (the default), \code{group_by} will ignore existing groups. If TRUE, add grouping variables to existing groups.
#' @param .dots Used to work around non-standard evaluation.
#'
#' @details
#' When called on an Xdf file, \code{group_by} does not do any data processing; it only sets up the necessary metadata for verbs accepting grouped tbls to handle the data correctly. When called on a non-Xdf data source, it imports the data into an Xdf tbl.
#'
#' Note that by default, the levels of the grouping variables for Xdf files are \emph{unsorted.}
#'
#' Most verbs that have specific methods for grouped data will split the data into multiple Xdf files, and then process each file separately (the exception is \code{\link[dplyrXdf]{summarise}}, which allows a range of options in how to treat groups). There are two options for splitting the data: use the \code{\link[RevoScaleR]{rxExecBy}} supplied in the RevoScaleR package, or via dplyrXdf-internal code. The former is the default if the version of Microsoft R installed is 9.1 or higher.
#'
#' @seealso
#' \code{\link[dplyr]{group_by}}, \code{\link{dplyrxdf_options}} for how to change the splitting procedure
#' @aliases group_by
#' @rdname group_by
#' @export
group_by.RxFileData <- function(.data, ..., add=FALSE)
{
    grps <- names(rlang::quos(..., .named=TRUE))
    if(length(grps) > 0)
    {
        .data <- rxImport(.data, tbl_xdf(.data), rowsPerRead=.dxOptions$rowsPerRead)
        .data <- as(.data, "grouped_tbl_xdf")
        .data@groups <- grps
    }
    .data
}


#' @rdname group_by
#' @export
group_by.RxXdfData <- function(.data, ..., add=FALSE)
{
    grps <- names(rlang::quos(..., .named=TRUE))
    if(length(grps) > 0)
    {
        composite <- is_composite_xdf(.data)
        .data <- as(.data, "grouped_tbl_xdf")
        .data@groups <- grps
        .data@hasTblFile <- FALSE # for raw xdfs, mark file as non-deletable
        .data@createCompositeSet <- composite
    }
    .data
}


#' @rdname group_by
#' @export
group_by.tbl_xdf <- function(.data, ..., add=FALSE)
{
    grps <- names(rlang::quos(..., .named=TRUE))
    if(length(grps) > 0)
    {
        .data <- as(.data, "grouped_tbl_xdf")
        .data@groups <- grps
    }
    .data
}


#' @rdname group_by
#' @export
group_by.grouped_tbl_xdf <- function(.data, ..., add=FALSE)
{
    grps <- names(rlang::quos(..., .named=TRUE))
    newGrps <- if(add) c(group_vars(.data), grps) else grps
    if(length(newGrps) > 0)
        .data@groups <- newGrps
    else .data <- as(.data, "tbl_xdf")
    .data
}


#' Get the groups for a file data source, or a tbl wrapping an Xdf file
#'
#' @param x A tbl for an Xdf data source; or a raw file data source.
#'
#' @return
#' If \code{x} is a grouped tbl, a character vector giving the grouping variable names; otherwise, \code{NULL}.
#' @rdname groups
#' @export
group_vars.RxFileData <- function(x)
{
    character(0)
}


#' @rdname groups
#' @export
group_vars.grouped_tbl_xdf <- function(x)
{
    x@groups
}


#' @rdname groups
#' @export
groups.RxFileData <- function(x)
{
    NULL
}


#' @rdname groups
#' @export
groups.grouped_tbl_xdf <- function(x)
{
    rlang::syms(group_vars(x))
}


#' @rdname groups
#' @export
ungroup.grouped_tbl_xdf <- function(x)
{
    as(x, "tbl_xdf")
}


#' @rdname groups
#' @export
ungroup.RxFileData <- function(x)
{
    x
}


