#' @include tbl_xdf.R
NULL

# assorted unexported functions
varTypes <- function(xdf, vars=NULL)
{
    sapply(rxGetVarInfo(xdf, varsToKeep=vars, computeInfo=FALSE), "[[", "varType")
}


# do not export this: arbitrarily changing the file pointer of an xdf object can be bad
`tblFile<-` <- function(x, value)
{
    if(!inherits(x, "tbl_xdf"))
        stop("bad call to 'tblFile<-': cannot change raw xdf file")
    x@file <- value
    x@hasTblFile <- TRUE
    x
}


# generate a new Xdf data source with file pointing to a random file, other parameters taken from input data source
newTbl <- function(xdf=NULL, fileSystem=rxGetFileSystem(xdf), tblDir=tempdir())
{
    fname <- if(inherits(fileSystem, "RxNativeFileSystem"))
        tempfile(tmpdir=tblDir, fileext=".xdf")
    else if(inherits(fileSystem, "RxHdfsFileSystem"))
    {
        # ensure HDFS temporary directory exists
        makeHdfsTempDir()
        file.path(.dxOptions$hdfsTempDir, basename(tempfile(pattern="xdfTbl")), fsep="/")
    }
    else stop("unknown file system")

    if(!inherits(xdf, "RxXdfData"))
        return(RxXdfData(file=fname, fileSystem=fileSystem))
    else xdf <- as(xdf, "RxXdfData")  # do coerce to remove any grouping info

    xdf@file <- fname
    xdf@fileSystem <- fileSystem
    xdf
}


# delete one or more xdf tbls (vectorised)
deleteTbl <- function(xdf)
{
    if(is.character(xdf))
        stop("must supply xdf data source or list of data sources")
    if(!is.list(xdf))
        xdf <- list(xdf)
    lapply(xdf, function(xdf) {
        filesystem <- rxGetFileSystem(xdf)
        filename <- xdf@file
        if(inherits(filesystem, "RxNativeFileSystem"))
        {
            # use unlink because file.remove can't handle directories on Windows
            # (composite xdfs are directories)
            if(file.exists(filename)) unlink(filename, recursive=TRUE)
        }
        else if(inherits(filesystem, "RxHdfsFileSystem"))
        {
            # xdf files in HDFS are always composite
            rxHadoopRemoveDir(filename, skipTrash=TRUE, intern=TRUE)
        }
        else stop("unknown file system, cannot remove file")
    })
    invisible(NULL)
}

