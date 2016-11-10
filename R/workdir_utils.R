#' @export
dxSetWorkDir <- function(dir, fileSystem=rxGetFileSystem())
{
    if(is.character(fileSystem))
    {
        if(fileSystem == "hdfs")
            fileSystem <- RxHdfsFileSystem()
        else fileSystem <- RxNativeFileSystem()
    }
    if(inherits(fileSystem, "RxHdfsFileSystem"))
    {
        if(missing(dir))
            dir <- gsub("\\", "/", tempfile(pattern="dxTmp", tmpdir="/tmp"), fixed=TRUE)
        .dxOptions$hdfsWorkDir <- dir
        .dxOptions$hdfsWorkDirCreated <- FALSE
    }
    else
    {
        if(missing(dir))
            dir <- tempdir()
        else
        {
            dir <- normalizePath(dir)
            dir.create(dir)
        }
        .dxOptions$localWorkDir <- dir
    }
    dir
}


#' @export
dxGetWorkDir <- function(fileSystem=rxGetFileSystem())
{
    if(is.character(fileSystem))
    {
        if(fileSystem == "hdfs")
            fileSystem <- RxHdfsFileSystem()
        else fileSystem <- RxNativeFileSystem()
    }
    if(inherits(fileSystem, "RxHdfsFileSystem"))
        .dxOptions$hdfsWorkDir
    else .dxOptions$localWorkDir
}

