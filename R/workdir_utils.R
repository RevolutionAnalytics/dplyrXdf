#' Get and set the xdf tbl directory
#'
#' @param dir Directory to use for saving xdf tbls
#' @param fileSystem The filesystem for which to set the tbl directory; can be either "hdfs" or "local". Currently only the local filesystem is supported.
#'
#' @details
#' By default, dplyrXdf will save the xdf files it creates to the R temporary directory. This can be a problem if the temporary directory is in a location with limited disk space. To change the directory to another location, use \code{setXdfTblDir}; to view the current location, use \code{getXdfTblDir}.
#'
#' @section warning: 
#' You should only set the tbl directory at the beginning of an R session. If you change the directory in the middle of a session, dplyrXdf will no longer be able to find its tbls, which means they may become inaccessible.
#' @export
setXdfTblDir <- function(dir, fileSystem=rxGetFileSystem())
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


#' @rdname setXdfTblDir
#' @export
getXdfTblDir <- function(fileSystem=rxGetFileSystem())
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

