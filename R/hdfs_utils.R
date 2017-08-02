#' @export
hdfs_dir <- function(path=".", ..., full_path=FALSE, include_dirs=FALSE, recursive=FALSE,
    dirs_only=FALSE, pattern=NULL, convert_backslashes=TRUE)
{
    path <- convertBS(path, convert_backslashes)
    arguments <- c("fs", "-ls", if(recursive) "-R", ..., path)

    # modified from rxHadoopListFiles
    cc <- rxGetComputeContext()
    result <- if(isRemoteHdfsClient())
        RevoScaleR:::executeOnHadoopCluster(cc, "hadoop", arguments)
    else RevoScaleR:::executeCommand("hadoop", arguments)

    if(result$rc != 0)
        stop(result$stderr)

    output <- grep("Found \\d+ item", result$stdout, invert=TRUE, value=TRUE)

    if(!recursive)
        include_dirs <- TRUE

    if(!include_dirs)
        output <- output[substr(output, 1, 1) != "d"]
    else if(dirs_only)
        output <- output[substr(output, 1, 1) == "d"]

    #output <- gsub("^[^/]*(/.*)$", "\\1", output)
    output <- substr(output, regexpr("[^ ]+$", output), nchar(output))

    if(!full_path && !recursive)
        output <- basename(output)
    if(!is.null(pattern))
        output <- grep(pattern, output, value=TRUE)

    attr(output, "path") <- path
    class(output) <- "dplyrXdf_hdfs_dir"
    output
}


#' @export
print.dplyrXdf_hdfs_dir <- function(x, ...)
{
    path <- attr(x, "path")
    if(substr(path, 1, 1) != "/")
        message("HDFS user directory assumed to be ", getHdfsUserDir())
    cat("Directory listing of", path, "\n")
    print.default(c(x), ...)
    invisible(x)
}


#' @export
hdfs_dir_exists <- function(path, convert_backslashes=TRUE)
{
    path <- convertBS(path, convert_backslashes)
    out <- suppressWarnings(try(hdfs_dir(path, "-d", dirs_only=TRUE), silent=TRUE))
    !inherits(out, "try-error") && length(out) > 0 && out == basename(path)
}


# for symmetry with hdfs_dir_exists
#' @export
hdfs_file_exists <- function(path, convert_backslashes=TRUE)
{
    path <- convertBS(path, convert_backslashes)
    rxHadoopFileExists(path)
}


#' @export
hdfs_dir_create <- function(path, ..., convert_backslashes=TRUE)
{
    path <- convertBS(path, convert_backslashes)
    rxHadoopMakeDir(path, ...)
}


#' @export
hdfs_dir_remove <- function(path, ..., convert_backslashes=TRUE)
{
    path <- convertBS(path, convert_backslashes)
    rxHadoopRemoveDir(path, ...)
}


#' @export
hdfs_file_copy <- function(src, dest, ..., overwrite=TRUE, convert_backslashes=TRUE)
{
    src <- convertBS(src, convert_backslashes)
    dest <- convertBS(dest, convert_backslashes)
    if(length(src) > 1 && length(dest) > 1)
        all(mapply(rxHadoopCopy, src, dest, MoreArgs=list(...)))
    else rxHadoopCopy(src, dest, ...)
}


#' @export
hdfs_file_remove <- function(path, ..., convert_backslashes=TRUE)
{
    path <- convertBS(path, convert_backslashes)
    rxHadoopRemove(path, ...)
}


#' @export
hdfs_file_move <- function(src, dest, ..., convert_backslashes=TRUE)
{
    src <- convertBS(src, convert_backslashes)
    dest <- convertBS(dest, convert_backslashes)
    if(length(src) > 1 && length(dest) > 1)
        all(mapply(rxHadoopMove, src, dest, MoreArgs=list(...)))
    else rxHadoopMove(src, dest, ...)
}


#' @export
hdfs_expunge <- function()
{
    rxHadoopCommand("fs -expunge")
}


#' @export
in_hdfs <- function(obj=NULL)
{
    fs <- rxGetFileSystem(obj)
    inherits(fs, "RxHdfsFileSystem") ||
        inherits(obj, "RxSparkData") ||
        (is.character(obj) && tolower(obj) == "hdfs")
}


#' @export
local_exec <- function(expr, context="local")
{
    cc <- rxGetComputeContext()
    if(inherits(cc, "RxDistributedHpa"))
    {
        on.exit(rxSetComputeContext(cc))
        rxSetComputeContext(context)
    }
    eval(expr, parent.frame())
}


# connected to remote cluster -> TRUE
# edge node -> FALSE
# not connected to cluster -> NA/error
isRemoteHdfsClient <- function(stopIfNotConnected=TRUE)
{
    cc <- rxGetComputeContext()
    onClusterNode <- try(cc@onClusterNode, silent=TRUE)
    if(inherits(onClusterNode, "try-error"))
    {
        # assume if hadoop executable found, then this is an edge node
        if(file.exists(rxGetOption("mrsHadoopPath")))
            return(TRUE)
        else
        {
            if(stopIfNotConnected)
                stop("not connected to HDFS filesystem", call.=FALSE)
            return(NA)
        }
    }
    else !isTRUE(onClusterNode)
}


# not all xdf functionality currently supported on HDFS
# rxSort, rxMerge, Pema, anything involving appends (which also means grouping)
stopIfHdfs <- function(.data, ...)
{
    if(in_hdfs(.data))
        stop(..., call.=FALSE)
}


getHdfsUserDir <- function(fs)
{
    # fail early if no HDFS found
    remote <- isRemoteHdfsClient()

    user <- if(remote)
        rxGetComputeContext()@sshUsername
    else Sys.info()["user"]

    paste0("/user/", user)
}


normalizeHdfsPath <- function(path)
{
    userDir <- getHdfsUserDir()
    path <- gsub("/\\./", "/", convertBS(path, TRUE))
    path <- sub("^\\./", "", path)
    #dir <- dirname(path)
    if(path == ".")
        userDir
    else if(substr(path, 1, 1) != "/")
        file.path(userDir, path, fsep="/")
    else path
}


convertBS <- function(path, convert)
{
    if(convert)
        gsub("\\\\", "/", path)
    else path
}
