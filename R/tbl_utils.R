#' @include tbl_xdf.R
NULL

#' Get the variable names or types for a RevScaleR data source
#'
#' @param x A data source object, or tbl wrapping the same.
#' @details
#' These are simple wrappers around the \code{names} and \code{rxGetVarInfo} functions.
#'
#' @return
#' For \code{tbl_vars}, a character vector of variable names; for \code{tbl_types}, a named vector giving the types of each variable.
#'
#' @seealso
#' \code{\link{RxXdfData}}, \code{\link{RxTextData}}, \code{\link{RxSasData}}, \code{\link{RxSpssData}},
#' \code{\link{rxGetInfo}}, \code{\link{rxGetVarInfo}}
#' @aliases tbl_vars
#' @rdname tbl_vars
#' @export
tbl_vars.RxFileData <- function(x)
{
    names(x)
}


#' @rdname tbl_vars
#' @export
tbl_types <- function(x)
{
    varTypes(x, NULL)
}


#' @rdname tbl_vars
#' @export
setMethod("names", c(x="tbl_xdf"), function(x)
{
    # deal with HDFS/tbl incompatibility: convert to raw RxXdfData, then convert back
    callNextMethod(as(x, "RxXdfData"))
})


#' @rdname tbl_vars
#' @export
setMethod("names<-", c(x="tbl_xdf", value="character"), function(x, value)
{
    # deal with HDFS/tbl incompatibility: convert to raw RxXdfData, then convert back
    cls <- class(x)
    hasTbl <- x@hasTblFile
    x <- as(x, "RxXdfData")
    names(x) <- value
    x <- as(x, cls)
    x@hasTblFile <- hasTbl
    x
})


#' @rdname tbl_vars
#' @export
setMethod("names<-", c(x="grouped_tbl_xdf", value="character"), function(x, value)
{
    # deal with HDFS/tbl incompatibility: convert to raw RxXdfData, then convert back
    cls <- class(x)
    grps <- group_vars(x)
    hasTbl <- x$hasTblFile
    x <- as(x, "RxXdfData")
    names(x) <- value
    x <- as(x, cls)
    x@groups <- grps
    x@hasTblFile <- hasTbl
    x
})


# assorted unexported functions
varTypes <- function(x, vars=NULL)
{
    if(!is.null(vars) && length(vars) == 0)  # handle no-variable input
        return(character(0))

    info <- if(is.data.frame(x))
        execOnHdfsClient(rxGetVarInfo(x, varsToKeep=vars, computeInfo=FALSE))
    else rxGetVarInfo(unTbl(x), varsToKeep=vars, computeInfo=FALSE)

    sapply(info, "[[", "varType")
}


# return list of factor levels (NULL if variable is not a factor)
varLevels <- function(x, vars=NULL)
{
    if(!is.null(vars) && length(vars) == 0) # handle no-variable input
        return(character(0))

    info <- if(is.data.frame(x))
        execOnHdfsClient(rxGetVarInfo(x, varsToKeep=vars, computeInfo=FALSE))
    else rxGetVarInfo(unTbl(x), varsToKeep=vars, computeInfo=FALSE)

    sapply(info, "[[", "levels", simplify=FALSE)
}


# delete one or more xdf tbls (vectorised)
deleteTbl <- function(xdf)
{
    if(!is.list(xdf))
        xdf <- list(xdf)
    lapply(xdf, function(xdf) {
        filesystem <- rxGetFileSystem(xdf)
        filename <- xdf@file
        if(inherits(filesystem, "RxNativeFileSystem"))
        {
            if(!file.exists(filename))
                warning("tbl file not found: ", filename)
            # use unlink because file.remove can't handle directories on Windows
            # (composite xdfs are directories)
            unlink(filename, recursive=TRUE)
        }
        else if(inherits(filesystem, "RxHdfsFileSystem"))
        {
            # xdf files in HDFS are always composite
            rxHadoopRemoveDir(filename, skipTrash=TRUE, intern=TRUE)
        }
        else stop("unknown file system, cannot remove file")
    })
    NULL
}


# on.exit function
deleteIfTbl <- function(data)
{
    # sanity check if passed a data frame
    if(is.data.frame(data))
        return(NULL)
    if((inherits(data, "tbl_xdf") && isTRUE(data@hasTblFile)) || is.list(data))
        deleteTbl(data)
    NULL
}


getTblFile <- function(data)
{
    if(is.data.frame(data))
        NULL
    else if(inherits(data, "RxFileData"))
        data@file
    else stop("not a local data source format", call.=FALSE)
}


