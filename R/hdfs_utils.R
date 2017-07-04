#' @export
hdfs_dir <- function(path=NULL, full_path=FALSE, dirs_only=FALSE, recursive=FALSE, ...)
{
    cc <- rxGetComputeContext()
    if(is.null(path))
    {
        user <- try(cc@sshUsername, silent=TRUE)
        if(inherits(user, "try-error"))
            user <- Sys.getenv("USER")  # Linux local CC
        if(user == "")
            user <- Sys.getenv("USERNAME")  # Windows local CC
        path <- file.path("/user", user, fsep="/")
    }

    arguments <- c("fs", "-ls", if(recursive) "-R", path)

    # taken from rxHadoopListFiles
    if(inherits(cc, c("RxLocalSeq", "RxLocalParallel", "RxForeachDoPar")))
    {
        result <- RevoScaleR:::executeCommand("hadoop", arguments)
    }
    else if(inherits(cc, "RxHadoopMR"))
    {
        result <- RevoScaleR:::executeOnHadoopCluster(cc, "hadoop", arguments)
    }
    else stop("listing Hadoop filesystem is not supported in the ", class(cc)[1], " compute context", call.=FALSE)

    if(result$rc != 0)
        stop(result$stderr)

    output <- grep("Found \\d+ item", result$stdout, invert=TRUE, value=TRUE)

    if(dirs_only)
        output <- output[substr(output, 1, 1) == "d"]
    output <- gsub("^[^/]*(/.*)$", "\\1", output)
    if(!full_path)
        output <- basename(output)
    attr(output, "path") <- path
    class(output) <- "dplyrXdf_hdfs_dir"
    output
}


print.dplyrXdf_hdfs_dir <- function(x, ...)
{
    path <- attr(x, "path")
    cat("Directory listing of", path, "\n")
    print.default(c(x), ...)
    invisible(x)
}

#' @export
copy_to.RxHadoopMR <- function(dest, df, name=deparse(substitute(df)), overwrite=FALSE, ...)
{
    if(inherits(df, "RxFileData") || is.data.frame(df))
        src <- df
    else if(is.character(name))
        src <- name
    rem <- isRemoteHdfsClient()
    if(is.na(rem))
        stop("not connected to HDFS filesystem")
    else if(rem)
        rxHadoopCopyFromClient(src, dest, ...)
    else rxHadoopCopyFromLocal(src, dest, ...)
}


#' @export
copy_to.RxHdfsFileSystem <- function(dest, df, name=deparse(substitute(df)), overwrite=FALSE, ...)
{
    if(inherits(df, "RxFileData") || is.data.frame(df))
        src <- df
    else if(is.character(name))
        src <- name
    rem <- isRemoteHdfsClient()
    if(is.na(rem))
        stop("not connected to HDFS filesystem")
    else if(rem)
        rxHadoopCopyFromClient(src, dest, ...)
    else rxHadoopCopyFromLocal(src, dest, ...)
}


# TRUE -> connected to remote cluster
# FALSE -> edge node
# NA -> not connected to cluster
isRemoteHdfsClient <- function()
{
    cc <- rxGetComputeContext()
    onClusterNode <- try(cc@onClusterNode, silent=TRUE)
    if(inherits(onClusterNode, "try-error"))
    {
        # assume if hadoop executable found, then this is an edge node
        if(Sys.which("hadoop") == "")
            return(NA)
        else return(TRUE)
    }
    else !isTRUE(onClusterNode)
}


# not all xdf functionality currently supported on HDFS
# rxSort, rxMerge, Pema, anything involving appends (which also means grouping)
stopIfHdfs <- function(.data, ...)
{
    if(isHdfs(rxGetFileSystem(.data)))
        stop(..., call.=FALSE)
}


# create the temporary directory in HDFS
# must run this every time we create a new tbl, because tempdir in HDFS is not guaranteed to exist
makeHdfsWorkDir <- function()
{
    if(.dxOptions$hdfsWorkDirCreated)  # quit if already created
        return()
    rxHadoopMakeDir(.dxOptions$hdfsWorkDir)
    .dxOptions$hdfsWorkDirCreated <- TRUE
    NULL
}


isHdfs <- function(fs)
{
    inherits(fs, "RxHdfsFileSystem") || (fs == "hdfs")
}
