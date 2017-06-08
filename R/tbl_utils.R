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


# assorted unexported functions
varTypes <- function(x, vars=NULL)
{
    sapply(rxGetVarInfo(x, varsToKeep=vars, computeInfo=FALSE), "[[", "varType")
}


# return list of factor levels (NULL if variable is not a factor)
varLevels <- function(x, vars=NULL)
{
    sapply(rxGetVarInfo(x, varsToKeep=vars, computeInfo=FALSE), "[[", "levels", simplify=FALSE)
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
        dplyrXdf:::deleteTbl(data)  # use explicit namespace for par compute contexts
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


#isTempTbl <- function(data)
#{
    #inherits(data, "tbl_xdf") && isTRUE(data@hasTblFile)
#}
