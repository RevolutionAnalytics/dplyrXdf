#' Utilities for HDFS
#'
#' Functions for working with files in HDFS: directory listing; file copy, move and delete; directory create and delete; test for file/directory existence; check if in HDFS; expunge Trash.
#'
#' @param path A HDFS pathname.
#' @param full_path For \code{hdfs_dir}, whether to prepend the directory path to filenames to give a full path. If FALSE, only file names are returned.
#' @param include_dirs For \code{hdfs_dir}, if subdirectory names should be included. Always TRUE for non-recursive listings.
#' @param recursive For \code{hdfs_dir}, if the listing should recurse into subdirectories.
#' @param dirs_only For \code{hdfs_dir} if \emph{only} subdirectory names should be included.
#' @param pattern For \code{hdfs_dir}, an optional \link{regular expression}. Only file names that match will be returned.
#' @param ... For \code{hdfs_dir}, further switches, prefixed by \code{"-"}, to pass to the Hadoop \code{fs -ls} command. For other functions, further arguments to pass to \code{\link{rxHadoopCommand.}}
#' @param host The HDFS hostname as a URI string, in the form \code{adl://host.name}. You should need to set this only if you have an attached Azure Data Lake Store that you are accessing via HDFS. Can also be specified as an \code{RxHdfsFileSystem} object, in which case the hostname will be taken from the object.
#' @param convert_backslashes Whether to convert any backslashes found in the input to forward slashes.
#' @param src,dest For \code{hdfs_file_copy} and \code{hdfs_file_move}, the source and destination paths.
#'
#' @details
#' These are utility functions to simplify working with files and directories in HDFS. For the most part, they wrap lower-level functions provided by RevoScaleR, which in turn wrap various Hadoop file system commands. They work with any file that is stored in HDFS, not just Xdf files.
#'
#' The \code{hdfs_dir} function is analogous to \code{dir} for the native filesystem. Like that function, and unlike \code{\link{rxHadoopListFiles}}, it returns a vector of filenames (\code{rxHadoopListFiles} returns a vector of \emph{printed output} from the \code{hadoop fs -ls} command, which is not quite the same thing). Again unlike \code{rxHadoopListFiles}, it does not print anything by default (the \code{print} method takes care of that).
#'
#' @return
#' \code{hdfs_dir} returns a vector of filenames, optionally with the full path attached.
#'
#' @seealso
#' \code{\link{dir}}, \code{link{dir.exists}}, \code{\link{file.exists}}, \code{\link{dir.create}},
#' \code{\link{file.copy}}, \code{\link{file.rename}}, \code{\link{file.remove}}, \code{\link{unlink}},
#' \code{\link{rxHadoopListFiles}}, \code{\link{rxHadoopFileExists}},
#' \code{\link{rxHadoopMakeDir}}, \code{\link{rxHadoopRemoveDir}},
#' \code{\link{rxHadoopCopy}}, \code{\link{rxHadoopMove}}, \code{\link{rxHadoopRemove}}
#' @rdname hdfs
#' @export
hdfs_dir <- function(path=".", ..., full_path=FALSE, include_dirs=FALSE, recursive=FALSE,
    dirs_only=FALSE, pattern=NULL, host=getHdfsHost(), convert_backslashes=TRUE)
{
    path <- makeHdfsUri(host, path, convert_backslashes)

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
    ## NOTE: regex below will break on filenames with a space
    output <- substr(output, regexpr("[^ ]+$", output), nchar(output))

    if(!full_path && !recursive)
        output <- basename(output)
    if(!is.null(pattern))
        output <- grep(pattern, output, value=TRUE)

    attr(output, "path") <- path
    class(output) <- "dplyrXdf_hdfs_dir"
    output
}


#' @rdname hdfs
#' @export
print.dplyrXdf_hdfs_dir <- function(x, ...)
{
    path <- attr(x, "path")

    if(!hasUriScheme(path) && substr(path, 1, 1) != "/")
        message("HDFS user directory assumed to be ", getHdfsUserDir())
    cat("Directory listing of", path, "\n")
    print.default(c(x), ...)
    invisible(x)
}


#' @details
#' \code{hdfs_dir_exists} and \code{hdfs_file_exists} test for the existence of a given directory and file, respectively. They are analogous to \code{dir.exists} and \code{file.exists} for the native filesystem.
#'
#' @return
#' \code{hdfs_dir_exists} and \code{hdfs_file_exists} return TRUE or FALSE depending on whether the directory or file exists.
#' @rdname hdfs
#' @export
hdfs_dir_exists <- function(path, host=getHdfsHost(), convert_backslashes=TRUE)
{
    detectHdfsConnection()
    path <- makeHdfsUri(host, path, convert_backslashes)
    out <- suppressWarnings(try(hdfs_dir(path, "-d", dirs_only=TRUE), silent=TRUE))
    !inherits(out, "try-error") && length(out) > 0 && out == basename(path)
}


#' @rdname hdfs
#' @export
hdfs_file_exists <- function(path, host=getHdfsHost(), convert_backslashes=TRUE)
{
    detectHdfsConnection()
    path <- makeHdfsUri(host, path, convert_backslashes)
    rxHadoopFileExists(path)
}


#' @details
#' \code{hdfs_dir_create} and \code{hdfs_dir_remove} create and remove directories. They are analogous to \code{dir.create} and \code{unlink(recursive=TRUE)} for the native filesystem.
#'
#' @return
#' The other \code{hdfs_*} functions return TRUE or FALSE depending on whether the operation succeeded.
#' @rdname hdfs
#' @export
hdfs_dir_create <- function(path, ..., host=getHdfsHost(), convert_backslashes=TRUE)
{
    detectHdfsConnection()
    path <- makeHdfsUri(host, path, convert_backslashes)
    rxHadoopMakeDir(path, ...)
}


#' @rdname hdfs
#' @export
hdfs_dir_remove <- function(path, ..., host=getHdfsHost(), convert_backslashes=TRUE)
{
    detectHdfsConnection()
    path <- makeHdfsUri(host, path, convert_backslashes)
    rxHadoopRemoveDir(path, ...)
}


#' @details
#' \code{hdfs_file_copy} and \code{hdfs_file_move} copy and move files. They are analogous to \code{file.copy} and \code{file.rename} for the native filesystem. Unlike \code{\link{rxHadoopCopy}} and \code{\link{rxHadoopMove}}, they are vectorised in both \code{src} and \code{dest}.
#'
#' Currently, RevoScaleR has only limited support for accessing multiple HDFS filesystems simultaneously. In particular, \code{src} and \code{dest} should both be on the same HDFS filesystem, whether host or ADLS.
#' @rdname hdfs
#' @export
hdfs_file_copy <- function(src, dest, ..., host=getHdfsHost(), convert_backslashes=TRUE)
{
    detectHdfsConnection()
    src <- makeHdfsUri(host, src, convert_backslashes)
    dest <- makeHdfsUri(host, dest, convert_backslashes)
    nSrc <- length(src)
    nDest <- length(dest)

    if(nSrc > 1 && nDest > 1 && nSrc == nDest)
        all(mapply(rxHadoopCopy, src, dest, MoreArgs=list(...)))
    else if(nDest > 1)
        stop("either supply one dest for each src, or exactly one dest")
    else rxHadoopCopy(src, dest, ...)
}


#' @rdname hdfs
#' @export
hdfs_file_move <- function(src, dest, ..., host=getHdfsHost(), convert_backslashes=TRUE)
{
    detectHdfsConnection()
    src <- makeHdfsUri(host, src, convert_backslashes)
    dest <- makeHdfsUri(host, dest, convert_backslashes)
    nSrc <- length(src)
    nDest <- length(dest)

    if(nSrc > 1 && nDest > 1 && nSrc == nDest)
        all(mapply(rxHadoopMove, src, dest, MoreArgs=list(...)))
    else if(nDest > 1)
        stop("either supply one dest for each src, or exactly one dest")
    else rxHadoopMove(src, dest, ...)
}


#' @details
#' \code{hdfs_file_remove} deletes files. It is analogous to \code{file.remove} and \code{unlink} for the native filesystem.
#' @rdname hdfs
#' @export
hdfs_file_remove <- function(path, ..., host=getHdfsHost(), convert_backslashes=TRUE)
{
    detectHdfsConnection()
    path <- makeHdfsUri(host, path, convert_backslashes)
    rxHadoopRemove(path, ...)
}


#' @details
#' \code{hdfs_expunge} empties the HDFS trash.
#' @rdname hdfs
#' @export
hdfs_expunge <- function()
{
    detectHdfsConnection()
    rxHadoopCommand("fs -expunge")
}


#' @param obj For \code{in_hdfs}, An R object, typically a RevoScaleR data source object.
#'
#' @return
#' \code{in_hdfs} returns whether the given object is stored in HDFS. This will be TRUE for an Xdf data source or file data source in HDFS, or a Spark data source. Classes for the latter include \code{RxHiveData}, \code{RxParquetData} and \code{RxOrcData}. If no argument is specified, it returns whether the default filesystem is HDFS.
#' @rdname hdfs
#' @export
in_hdfs <- function(obj=NULL)
{
    fs <- rxGetFileSystem(obj)
    inherits(fs, "RxHdfsFileSystem") || inherits(obj, "RxSparkData")
}


#' Runs an expression in the local compute context
#' 
#' @param expr An expression to execute. Normally something that depends on the compute context, such as \code{rxDataStep}.
#' @param context The compute context in which to execute \code{expr}. Defaults to local.
#'
#' @details
#' This function is useful when you are working with datasets in both the native filesystem and HDFS. The workhorse RevoScaleR function for data transformation, \code{rxDataStep}, will complain if you are in a distributed compute context such as \code{\link{RxHadoopMR}} or \code{\link{RxSpark}}, and you want to process a dataset in the native filesystem. You can wrap your code inside a \code{local_exec} call to switch to the local compute context temporarily, and then switch back when it has finished running.
#'
#' @return
#' The value of \code{expr}.
#'
#' @seealso
#' \code{\link{eval}}
#' @rdname local_exec
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
            return(FALSE)
        else
        {
            if(stopIfNotConnected)
                stop("not connected to HDFS", call.=FALSE)
            return(NA)
        }
    }
    else !isTRUE(onClusterNode)
}


# better name for this use case
detectHdfsConnection <- function(stopIfNotConnected=TRUE)
{
    isRemoteHdfsClient(stopIfNotConnected)
}


# not all xdf functionality currently supported on HDFS
# rxSort, Pema, anything involving appends (which also means grouping)
stopIfHdfs <- function(.data, ...)
{
    if(in_hdfs(.data))
        stop(..., call.=FALSE)
}


# counterpart to stopIfHdfs: some ops work with HDFS data but not in Hadoop/Spark CC
stopIfDistribCC <- function(...)
{
    if(inherits(rxGetComputeContext(), "RxHadoopMR"))
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


# get the HDFS host: needed to differentiate between "native" and Azure Data Lake storage
getHdfsHost <- function(fs=NULL)
{
    if(inherits(fs, "RxHdfsFileSystem"))
        return(fs$hostName)

    host <- try(rxGetComputeContext()@nameNode, silent=TRUE)
    if(!inherits(host, "try-error"))
        return(host)

    host <- rxGetFileSystem()$hostName
    if(!is.null(host))
        return(host)

    rxGetOption("hdfsHost")
}


normalizeHdfsPath <- function(path)
{
    path <- gsub("/\\./", "/", convertBS(path, TRUE))
    path <- sub("^\\./", "", path)

    # if path contains a scheme, don't change it
    if(hasUriScheme(path))
        return(path)

    userDir <- getHdfsUserDir()
    if(path == ".")
        userDir
    else if(substr(path, 1, 1) != "/")
        file.path(userDir, path, fsep="/")
    else path
}


makeHdfsUri <- function(host, path, convert=TRUE)
{
    if(inherits(host, "RxHdfsFileSystem"))
        host <- host$hostName
    path <- convertBS(path, convert)
    if(hasUriScheme(host) && !hasUriScheme(path))
        gsub("(?<!:)//", "/", file.path(host, path, fsep="/"), perl=TRUE)
    else path
}


convertBS <- function(path, convert)
{
    if(convert)
        gsub("\\\\", "/", path)
    else path
}


hasUriScheme <- function(path)
{
    if(length(path) > 0)
        grepl("^([[:alpha:]+.-]+):", path)
    else FALSE
}

