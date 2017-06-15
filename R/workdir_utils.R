#' Get and set the xdf tbl directory
#'
#' By default, dplyrXdf will save the xdf files it creates in the R temporary directory. This can be a problem if it is in a location with limited disk space. Use \code{set_dplyrxdf_dir} to change the xdf tbl directory, and \code{get_dplyrxdf_dir} to view it.
#'
#' @param path Location in which to save xdf tbls. If missing, defaults to the R temporary directory.
#' @param fileSystem The filesystem for which to set or get the tbl directory; can be either "hdfs" or "native". Currently only the native (local) filesystem is supported.
#'
#' @details
#' If \code{path} is supplied, \code{set_dplyrxdf_dir} creates a new directory (with a unique name) located \emph{under} \code{path}. This ensures that the files managed by dplyrXdf are properly isolated from the rest of the filesystem.
#'
#' @seealso
#' \code{\link{rxGetFileSystem}}, \code{\link{rxSetFileSystem}}
#' @rdname workdir
#' @export
set_dplyrxdf_dir <- function(path, fileSystem=rxGetFileSystem())
{
    if(is.character(fileSystem))
    {
        if(fileSystem == "hdfs")
            fileSystem <- RxHdfsFileSystem()
        else fileSystem <- RxNativeFileSystem()
    }
    if(inherits(fileSystem, "RxHdfsFileSystem"))
    {
        if(missing(path))
            path <- gsub("\\", "/", tempfile(pattern="dxTmp", tmpdir="/tmp"), fixed=TRUE)
        .dxOptions$hdfsWorkDir <- path
        .dxOptions$hdfsWorkDirCreated <- FALSE
    }
    else
    {
        if(missing(path))
            path <- tempdir()
        else
        {
            path <- tempfile(pattern="dxTmp", tmpdir=path)
            path <- normalizePath(path, mustWork=FALSE)
            dir.create(path, recursive=TRUE)
        }
        .dxOptions$localWorkDir <- path
    }
    invisible(NULL)
}


#' @rdname workdir
#' @export
get_dplyrxdf_dir <- function(fileSystem=rxGetFileSystem())
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

