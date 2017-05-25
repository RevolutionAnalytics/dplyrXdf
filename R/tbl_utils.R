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


## do not export this: arbitrarily changing the file pointer of an xdf object can be bad
#`tblFile<-` <- function(x, value)
#{
    #if(!inherits(x, "tbl_xdf"))
        #stop("bad call to 'tblFile<-': cannot change raw xdf file")
    #x@file <- value
    #x@hasTblFile <- TRUE
    #x
#}


## generate a new Xdf data source with file pointing to a random file, other parameters taken from input data source
#newTbl <- function(xdf=NULL, fileSystem=rxGetFileSystem(xdf), tblDir=getXdfTblDir())
#{
    #fname <- if(inherits(fileSystem, "RxNativeFileSystem"))
        #tempfile(tmpdir=tblDir, fileext=".xdf")
    #else if(inherits(fileSystem, "RxHdfsFileSystem"))
    #{
        ## ensure HDFS temporary directory exists
        #makeHdfsWorkDir()
        #file.path(.dxOptions$hdfsWorkDir, basename(tempfile(pattern="xdfTbl")), fsep="/")
    #}
    #else stop("unknown file system")

    #if(!inherits(xdf, "RxXdfData"))
        #return(RxXdfData(file=fname, fileSystem=fileSystem))
    #else xdf <- as(xdf, "RxXdfData")  # do coerce to remove any grouping info

    #xdf@file <- fname
    #xdf@fileSystem <- fileSystem
    #xdf
#}


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
deleteIfTbl <- function(oldData)
{
    # sanity check if passed a data frame
    if(is.data.frame(oldData))
        return(NULL)
    if(isTempTbl(oldData) || is.list(oldData))
        deleteTbl(oldData)
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


isTempTbl <- function(data)
{
    inherits(data, "tbl_xdf") && isTRUE(data@hasTblFile)
}
