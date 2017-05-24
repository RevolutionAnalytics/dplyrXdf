#' @include tbl_xdf.R
NULL


#' @exportClass grouped_tbl_xdf
grouped_tbl_xdf <- setClass("grouped_tbl_xdf", contains="tbl_xdf", slots=c(groups="characterORNULL"))


#setMethod("initialize", "grouped_tbl_xdf", function(.Object, groups=NULL, ...) {
    #.Object <- callNextMethod(.Object, ...)
    #.Object@groups <- groups
    #.Object
#})




#' Group an Xdf file by one or more variables
#'
#' @param .data An Xdf file or a tbl wrapping the same.
#' @param ... Variables to group by.
#' @param add If FALSE (the default), \code{group_by} will ignore existing groups. If TRUE, add grouping variables to existing groups.
#' @param .dots Used to work around non-standard evaluation.
#'
#' @details
#' \code{group_by} itself does not do any data processing; it only sets up the necessary metadata for verbs accepting grouped tbls to generate the groups.
#'
#' Most verbs that accept grouped data as input will split the data into multiple Xdf files, and then process each file separately. The exception is \code{\link[dplyrXdf]{summarise}}, which allows a range of options in how to treat groups.
#'
#' @seealso
#' \code{\link[dplyr]{group_by}}
#' @aliases group_by
#' @rdname group_by
#' @export
group_by.RxFileData <- function(.data, ..., add=FALSE)
{
    .data <- rxImport(.data, tbl_xdf(.data), rowsPerRead=.dxOptions$rowsPerRead)
    grps <- names(rlang::quos(..., .named=TRUE))
    .data <- as(.data, "grouped_tbl_xdf")
    .data@groups <- grps
    .data
}


#' @rdname group_by
#' @export
group_by.RxXdfData <- function(.data, ..., add=FALSE)
{
    grps <- names(rlang::quos(..., .named=TRUE))
    .data <- as(.data, "grouped_tbl_xdf")
    .data@groups <- grps
    .data@hasTblFile <- FALSE  # for raw xdfs, mark file as non-deletable
    .data
}


#' @rdname group_by
#' @export
group_by.tbl_xdf <- function(.data, ..., add=FALSE)
{
    grps <- names(rlang::quos(..., .named=TRUE))
    .data <- as(.data, "grouped_tbl_xdf")
    .data@groups <- grps
    .data
}


#' @rdname group_by
#' @export
group_by.grouped_tbl_xdf <- function(.data, ..., add=FALSE)
{
    oldGrps <- group_vars(.data)
    grps <- names(rlang::quos(..., .named=TRUE))
    .data <- as(.data, "grouped_tbl_xdf")
    .data@groups <- if(add) c(oldGrps, grps) else grps
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
    syms(group_vars(x))
}


#' @export
ungroup.grouped_tbl_xdf <- function(x)
{
    as(x, "tbl_xdf")
}


#' @export
ungroup.RxFileData <- function(x)
{
    x
}


get_grouplevels <- function(data, gvars=group_vars(data))
{
    stopIfHdfs("get_grouplevels not supported on HDFS")  # should never trip this

    if(is.null(gvars))
        return(NULL)

    levdf <- as.data.frame(sapply(gvars, function(xi) logical(0), simplify=FALSE))

    # read grouping variables by block, return unique row combinations
    levs <- rxDataStep(data, varsToKeep=gvars, transformFunc=function(varlst) {
        .levdf <<- dplyr::distinct(rbind(.levdf, as.data.frame(varlst)))
        NULL
    }, transformObjects=list(.levdf=levdf), transformPackages="dplyr", returnTransformObjects=TRUE)[[1]]

    levs <- do.call(paste, c(levs, sep="_&&_"))
    levs
}


make_groupvar <- function(gvars, levs)
{
    factor(do.call(paste, c(gvars, sep="_&&_")), levels=levs)
}


# paste individual groups back together
combine_groups <- function(datlst, output, grps)
{
    out <- if(inherits(datlst[[1]], "data.frame"))
        combine_group_dfs(datlst, output)
    else combine_group_xdfs(datlst, output)
    simpleRegroup(out, grps)
}


# paste individual group xdfs back together
combine_group_xdfs <- function(xdflst, output, grps)
{
    on.exit(deleteIfTbl(xdflst))
    xdf1 <- xdflst[[1]]

    dropvars <- if(".group." %in% names(xdf1)) ".group." else NULL

    # use rxDataStep loop for appending instead of rxMerge; latter is surprisingly slow
    stopIfHdfs("combine_group not supported on HDFS")  # should never trip this
    for(xdf in xdflst[-1])
        rxDataStep(xdf, xdf1, append="rows", computeLowHigh=FALSE)


    if(missing(output)) # xdf tbl
    {
        output <- newTbl(data)
    }
    else if(!is.null(output))
    {
        tmp <- xdf1
        tmp@file <- output
        output <- tmp
    }
    
    rxDataStep(xdf1, output, varsToDrop=dropvars, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE)
}


# paste individual group data frames back together
combine_group_dfs <- function(dflst, output)
{
    if(is.null(output))
        bind_rows(dflst)
    else rxDataStep(bind_rows(dflst), output, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE)
}


