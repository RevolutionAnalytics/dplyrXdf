#' @export
hdfs_dir <- function(path=".", ..., full_path=FALSE, include_dirs=FALSE, recursive=FALSE, dirs_only=FALSE)
{
    cc <- rxGetComputeContext()
    arguments <- c("fs", "-ls", if(recursive) "-R", ..., path)

    # modified from rxHadoopListFiles
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
    attr(output, "path") <- path
    class(output) <- "dplyrXdf_hdfs_dir"
    output
}


#' @export
hdfs_dir_exists <- function(path)
{
    out <- suppressWarnings(try(hdfs_dir(path, "-d", dirs_only=TRUE), silent=TRUE))
    !inherits(out, "try-error") && length(out) > 0 && out == path
}


#' @export
print.dplyrXdf_hdfs_dir <- function(x, ...)
{
    path <- attr(x, "path")
    cat("Directory listing of", path, "\n")
    print.default(c(x), ...)
    invisible(x)
}


#' @export
in_hdfs <- function(obj=NULL)
{
    fs <- rxGetFileSystem(obj)
    inherits(fs, "RxHdfsFileSystem") || (is.character(obj) && tolower(obj) == "hdfs")
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
    dir <- dirname(path)
    if(dir == ".")
        file.path(userDir, basename(path), fsep="/")
    else path
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



