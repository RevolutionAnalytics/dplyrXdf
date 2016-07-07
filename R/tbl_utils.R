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
newTbl <- function(xdf=NULL, fileSystem=rxGetFileSystem(xdf))
{
    fname <- if(inherits(fileSystem, "RxNativeFileSystem"))
        tempfile(fileext=".xdf")
    else if(inherits(fileSystem, "RxHdfsFileSystem"))
    {
        # ensure HDFS temporary directory exists
        makeHdfsTempDir()
        file.path(hdfsTempDir, basename(tempfile(fileext=".xdf")), fsep="/")
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
        stop("must supply xdf file or list of xdf files")
    if(!is.list(xdf))
        xdf <- list(xdf)
    lapply(xdf, function(xdf) {
        filesystem <- rxGetFileSystem(xdf)
        filename <- xdf@file
        if(inherits(filesystem, "RxNativeFileSystem"))
        {
            # use unlink because file.remove can't handle directories on Windows
            if(file.exists(filename)) unlink(filename, recursive=TRUE)
        }
        else if(inherits(filesystem, "RxHdfsFileSystem"))
        {
            # files in HDFS are always composite
            rxHadoopRemoveDir(filename)
        }
        else stop("unknown file system, cannot remove file")
    })
    invisible(NULL)
}


# create the temporary directory in HDFS
# must run this every time we create a new tbl, because tempdir in HDFS is not guaranteed to exist
makeHdfsTempDir <- function()
{
    hdfsTempDir <- .dxOptions$hdfsTempDir
    # create temp directory in HDFS
    # this behaviour is technically undocumented, but better than blindly trying to create dir
    if(rxHadoopListFiles(hdfsTempDir) == 1)
        rxHadoopMakeDir(hdfsTempDir)
    .dxOptions$hdfsTempDirCreated <- TRUE
    NULL
}


# environment for storing options
.dxOptions <- new.env(parent=emptyenv())


.dxInit <- function()
{
    # set the HDFS temporary directory
    hdfsTempDir <- tempfile(pattern="dxTmp", tmpdir=RxHadoopMR()@hdfsShareDir)
    .dxOptions$hdfsTempDir <- gsub("\\", "/", hdfsTempDir, fixed=TRUE)
    .dxOptions$hdfsTempDirCreated <- FALSE
    .dxOptions$rowsPerRead <- 500000

    defaultFS <- rxGetFileSystem()
    if(inherits(defaultFS, "RxHdfFileSystem"))
        makeHdfsTempDir()

    # store dplyr version
    .dxOptions$dplyrVersion <- packageVersion("dplyr")

    NULL
}


.dxFinal <- function(e)
{
    # remove the HDFS temporary directory
    if(e$hdfsTempDirCreated)
        rxHadoopRemoveDir(e$hdfsTempDir, skipTrash=TRUE)
    NULL
}


dxOptions <- function(...)
{
    if(nargs() == 0)
        return(as.list(.dxOptions))
    dots <- list(...)
    nams <- names(dots)
    opts <- names(.dxOptions)
    if(!all(nams %in% opts))
    {
        badOpts <- nams[!(nams %in% opts)]
        stop("invalid dplyrXdf option(s): ", paste(badOpts, collapse=", "))
    }
    for(o in nams)
        .dxOptions[[o]] <- dots[[o]]
    as.list(.dxOptions)
}
