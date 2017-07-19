#' @export
hdfs_dir <- function(path=".", full_path=FALSE, dirs_only=FALSE, recursive=FALSE, ...)
{
    cc <- rxGetComputeContext()
    arguments <- c("fs", "-ls", if(recursive) "-R", path)

    # modified from rxHadoopListFiles
    result <- if(isRemoteHdfsClient())
        RevoScaleR:::executeOnHadoopCluster(cc, "hadoop", arguments)
    else RevoScaleR:::executeCommand("hadoop", arguments)

    if(result$rc != 0)
        stop(result$stderr)

    output <- grep("Found \\d+ item", result$stdout, invert=TRUE, value=TRUE)

    if(dirs_only)
        output <- output[substr(output, 1, 1) == "d"]

    #output <- gsub("^[^/]*(/.*)$", "\\1", output)
    output <- substr(output, regexpr("[^ ]+$", output), nchar(output))

    if(!full_path && !recursive)
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
        if(Sys.which("hadoop") == "")
        {
            if(stopIfNotConnected)
                stop("not connected to HDFS filesystem", call.=FALSE)
            return(NA)
        }
        else return(TRUE)
    }
    else !isTRUE(onClusterNode)
}


# not all xdf functionality currently supported on HDFS
# rxSort, rxMerge, Pema, anything involving appends (which also means grouping)
stopIfHdfs <- function(.data, ...)
{
    if(isHdfs(.data))
        stop(..., call.=FALSE)
}


isHdfs <- function(obj)
{
    inherits(obj, "RxHdfsFileSystem") ||
        (inherits(obj, "RxFileData") && inherits(rxGetFileSystem(obj), "RxHdfsFileSystem")) ||
        (is.character(obj) && tolower(obj) == "hdfs")
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


execOnHdfsClient <- function(expr)
{
    cc <- rxGetComputeContext()
    if(inherits(cc, "RxDistributedHpa"))
    {
        on.exit(rxSetComputeContext(cc))
        rxSetComputeContext("local")
    }
    eval(expr, parent.frame())
}



