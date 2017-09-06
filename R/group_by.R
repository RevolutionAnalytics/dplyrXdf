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
#'
#' @details
#' When called on an Xdf file, \code{group_by} does not do any data processing; it only sets up the necessary metadata for verbs accepting grouped tbls to handle the data correctly. When called on a non-Xdf data source, it imports the data into an Xdf tbl.
#'
#' Note that by default, the levels of the grouping variables for Xdf files are \emph{unsorted.} This is for performance reasons, to avoid having to make unnecessary passes through the data.
#'
#'  There are two options for handling grouped data: use the \code{\link[RevoScaleR]{rxExecBy}} function supplied in the RevoScaleR package, or via dplyrXdf-internal code. The former is the default if the version of Microsoft R installed is 9.1 or higher.
#'
#' @section Parallel by-group processing:
#' Most verbs that have specific methods for grouped data will split the data into multiple Xdf files, and then process each file separately (the exception is \code{\link[dplyrXdf]{summarise}}. This makes it easy to parallelise the processing of groups in dplyrXdf. There are a number of options available:
#' \itemize{
#'     \item By default, if Microsoft R Server 9.1 or higher is installed, dplyrXdf will use \code{\link{rxExecBy}} to process groups. This will create a cluster of slave nodes in the background and send the data to the nodes by group. The cluster is destroyed at the end of each pipeline.
#'     \item For more flexibility, you can set the compute context manually to \code{\link{RxForeachDoPar}}. This will create a \emph{persistent} cluster that can be reused for multiple pipelines. The ForeachDoPar compute context can also use clusters made up of multiple machines, not just multiple processes on the single machine; see below for an example of this.
#'     \item If your data is stored in a Hadoop or Spark cluster, dplyrXdf will similarly take advantage of the Hadoop and Spark compute contexts to process data in parallel on the worker nodes.
#' }
#'
#' @seealso
#' \code{\link[dplyr]{group_by}} in package dplyr, \code{\link{dplyrxdf_options}} for how to change the splitting procedure
#'
#' @examples
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#' tbl <- group_by(mtx, cyl)
#' groups(tbl)
#' group_vars(tbl)
#'
#' ## parallel processing of groups with ForeachDoPar compute context
#' \dontrun{
#' flx <- as_tbl(nycflights13::flights)
#'
#' doParallel::registerDoParallel(3)
#' dplyrxdf_options(useExecBy=FALSE)  # turn rxExecBy processing off
#' rxSetComputeContext("dopar")
#'
#' flx %>%
#'     group_by(carrier) %>%
#'     do(m=lm(arr_time ~ dep_time + dep_delay + factor(month), data=.))
#'
#' doParallel::stopImplicitCluster()
#'
#' # ForeachDoPar also works with a cluster of multiple machines, not just multiple processes on one machine
#' # to work with dplyrXdf, all machines must have access to the same filesystem (eg a network share)
#' # doAzureParallel is available from GitHub: https://github.com/Azure/doAzureParallel
#' cl <- doAzureParallel::makeCluster("cluster.json")
#' doAzureParallel::registerDoAzureParallel(cl)
#'
#' # set the dplyrXdf working directory to a cluster-accessible location
#' set_dplyrxdf_dir("n:/clusterdata")
#'
#' flx2 <- as_xdf(nycflights13::flights, file="n:/clusterdata/flights.xdf")
#' flx2 %>%
#'     group_by(carrier) %>%
#'     do(m=lm(arr_time ~ dep_time + dep_delay + factor(month), data=.))
#' 
#' doAzureParallel::stopCluster(cl)
#' rxSetComputeContext("local")
#' dplyrxdf_options(useExecBy=TRUE)  # re-enable rxExecBy processing once we are done
#' }
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


simpleRegroup <- function(x, grps=character(0))
{
    # if x is raw xdf, don't save grouping info
    if(length(grps) > 0 && inherits(x, c("tbl_xdf", "data.frame")))
        group_by_at(x, grps)
    else x
}

