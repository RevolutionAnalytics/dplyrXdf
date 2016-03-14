#' @include tbl_xdf.R
NULL


#' @exportClass grouped_tbl_xdf
grouped_tbl_xdf <- setClass("grouped_tbl_xdf", contains="tbl_xdf", slots=c(groups="characterORNULL"))


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
#' Most verbs that accept grouped data as input will use \code{\link{rxSplit}} to split the data into multiple Xdf files, and then process each file separately. The exception is \code{\link[dplyrXdf]{summarise}}, which allows a range of options in how to treat groups.
#'
#' @seealso
#' \code{\link[dplyr]{group_by}}
#' @aliases group_by group_by_
#' @rdname group_by
#' @export
group_by_.RxFileData <- function(.data, ..., .dots, add=FALSE)
{
    group_by_(tbl(.data), ..., .dots=.dots, add=add)
}


#' @export
group_by_.RxXdfData <- function(.data, ..., .dots, add=FALSE)
{
    new_groups <- lazyeval::all_dots(.dots, ...)

    # identify Revo-specific arguments
    if(any(names(new_groups) == ".rxArgs"))
    {
        warning("group_by doesn't support .rxArgs argument", call.=FALSE)
        new_groups[[".rxArgs"]] <- NULL
    }

    # logic copied from dplyr:::group_by_prepare, dplyr:::names2, dplyr:::`%||%`
    is_name <- vapply(new_groups, function(x) is.name(x$expr), logical(1))
    has_name <- if(is.null(names(new_groups)))
        rep(FALSE, length(new_groups))
    else names(new_groups) != ""
    needs_mutate <- has_name | !is_name
    if(any(needs_mutate))
    {
        if(!inherits(.data, "RxXdfData"))
            stop("can only group by named variables for non-Xdf data sources")
        .data <- mutate_(.data, .dots=new_groups[needs_mutate])
    }

    new_groups <- lazyeval::auto_name(new_groups)
    groups <- names(new_groups)
    if(add)
        groups <- c(groups(.data), groups)
    groups <- groups[!duplicated(groups)]


    .data <- as(.data, "grouped_tbl_xdf")
    .data@hasTblFile <- hasTblFile(.data)
    .data@groups <- groups
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
groups.RxFileData <- function(x)
{
    NULL
}


#' @rdname groups
#' @export
groups.grouped_tbl_xdf <- function(x)
{
    x@groups
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


#get_grouplevels <- function(data)
#{
    #gvars <- groups(data)
    #if(is.null(gvars))
        #return(NULL)
    #RevoPemaR::pemaCompute(pemaGrplevels(), data=data, gvars=gvars)
#}


get_grouplevels <- function(data)
{
    gvars <- groups(data)
    if(is.null(gvars))
        return(NULL)
    # read grouping vars into memory
    levs <- rxDataStep(data, varsToKeep=gvars, maxRowsByCols=NULL, transformFunc=function(varlst) {
        l <- do.call(paste, c(varlst, sep="_&&_"))
        list(levs=l)
    })[[1]]
    unique(levs)
}


make_groupvar <- function(gvars, levs)
{
    factor(do.call(paste, c(gvars, sep="_&&_")), levels=levs)
}


# split an xdf into multiple xdfs, one for each group
split_groups <- function(data, outXdf)
{
    if(is.character(outXdf))
        stop("must supply output data source to split_groups")
    grps <- groups(data)
    levs <- get_grouplevels(data)
    data <- rxDataStep(data, outXdf, transformFunc=function(varlst) {
        varlst[[".group."]] <- .factor(varlst, .levs)
        varlst
    }, transformObjects=list(.levs=levs, .factor=make_groupvar), transformVars=grps, overwrite=TRUE)

    # rxSplit not supported on HDFS -- fake it with multiple rxDataSteps
    # this will be very slow with large no. of factor levels
    if(inherits(rxGetFileSystem(data), "RxHdfsFileSystem"))
    {
        outFile <- rxXdfFileName(outXdf)
        lst <- sapply(levs, function(l) {
            thisXdf <- outXdf
            thisXdf@file <- paste(sub(".xdf$", "", outFile), "_lev_", l, ".xdf", sep="")
            cl <- substitute(rxDataStep(data, thisXdf, rowsToKeep=.group. == .l, overwrite=TRUE),
                list(.l=l))
            eval(cl)
        }, simplify=FALSE)
    }
    else lst <- rxSplit(data, outFilesBase=rxXdfFileName(outXdf), splitByFactor=".group.")

    lapply(lst, function(obj) new("tbl_xdf", obj, hasTblFile=TRUE)) 
}


# paste individual groups back together
combine_groups <- function(datlst, outXdf, groups)
{
    xdf <- if(inherits(datlst[[1]], "data.frame"))
        combine_group_dfs(datlst, outXdf)
    else combine_group_xdfs(datlst, outXdf)

    # generate new grouping info if necessary
    numGroups <- length(groups)
    if(numGroups > 1)
        group_by_(xdf, .dots=groups[-numGroups])
    else xdf
}


# paste individual group xdfs back together
combine_group_xdfs <- function(xdflst, outXdf)
{
    on.exit(deleteTbl(xdflst))

    xdf1 <- xdflst[[1]]

    # use rxDataStep loop for appending instead of rxMerge; latter is surprisingly slow and unsupported on HDFS
    for(xdf in xdflst[-1])
        rxDataStep(xdf, xdf1, append="rows", computeLowHigh=FALSE)

    dropvars <- base::intersect(".group.", names(xdf1))
    tbl(xdf1, outXdf, rowsPerRead=1e6, varsToDrop=dropvars, overwrite=TRUE, hasTblFile=TRUE)
}


# paste individual group data frames back together
combine_group_dfs <- function(dflst, outXdf)
{
    xdf <- rxDataStep(bind_rows(dflst), outXdf, overwrite=TRUE)
    tbl(xdf, file=NULL, hasTblFile=TRUE)
}


#' @importFrom RevoPemaR setPemaClass
#' @importClassesFrom RevoPemaR PemaBaseClass
pemaGrplevels <- RevoPemaR::setPemaClass("PemaGrplevels",
    contains="PemaBaseClass",

    fields=list(levs="character", x="character", sep="character"),

    methods=list(
        initialize=function(gvars="", sep="_&&_", ...) {
            callSuper(...)
            usingMethods(.pemaMethods)
            levs <<- character(0)
            x <<- gvars
            sep <<- sep
            invisible(NULL)
        },

        processData=function(chunk) {
            grp <- do.call(paste, c(chunk[x], sep=sep))
            levs <<- unique(c(levs, grp))
            invisible(NULL)
        },

        updateResults=function(worker) {
            levs <<- unique(c(levs, worker$levs))
            invisible(NULL)
        },

        processResults=function() {
            sort(levs)
        },
        
        getVarsToUse=function() {
            x
        })
)


