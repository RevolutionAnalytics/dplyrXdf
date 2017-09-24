#' Utilities for HDFS
#'
#' Functions for working with files in HDFS: directory listing; file copy, move and delete; directory create and delete; test for file/directory existence; check if in HDFS; expunge Trash.
#'
#' @param path A HDFS pathname.
#' @param full_path For \code{hdfs_dir}, whether to prepend the directory path to filenames to give a full path. If FALSE, only file names are returned.
#' @param include_dirs For \code{hdfs_dir}, if subdirectory names should be included. Always TRUE for non-recursive listings.
#' @param recursive For \code{hdfs_dir}, if the listing should recurse into subdirectories.
#' @param dirs_only For \code{hdfs_dir}, if \emph{only} subdirectory names should be included.
#' @param pattern For \code{hdfs_dir}, an optional \link{regular expression}. Only file names that match will be returned.
#' @param ... For \code{hdfs_dir}, further switches, prefixed by \code{"-"}, to pass to the Hadoop \code{fs -ls} command. For other functions, further arguments to pass to \code{\link{rxHadoopCommand.}}
#' @param host The HDFS hostname as a string, in the form \code{adl://host.name}. You should need to set this only if you have an attached Azure Data Lake Store that you are accessing via HDFS. Can also be an \code{RxHdfsFileSystem} object, in which case the hostname will be taken from the object.
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
#' \code{\link{dir}}, \code{\link{dir.exists}}, \code{\link{file.exists}}, \code{\link{dir.create}},
#' \code{\link{file.copy}}, \code{\link{file.rename}}, \code{\link{file.remove}}, \code{\link{unlink}},
#' \code{\link{rxHadoopListFiles}}, \code{\link{rxHadoopFileExists}},
#' \code{\link{rxHadoopMakeDir}}, \code{\link{rxHadoopRemoveDir}},
#' \code{\link{rxHadoopCopy}}, \code{\link{rxHadoopMove}}, \code{\link{rxHadoopRemove}}
#'
#' @examples
#' \dontrun{
#' hdfs_host()
#'
#' mtx <- as_xdf(mtcars, overwrite=TRUE)
#' mth <- copy_to_hdfs(mtx)
#' in_hdfs(mtx)
#' in_hdfs(mth)
#' hdfs_host(mth)
#'
#' # always TRUE
#' hdfs_dir_exists("/")
#' # should always be TRUE if Microsoft R is installed on the cluster
#' hdfs_dir_exists("/user/RevoShare")
#'
#' # listing of home directory: /user/<username>
#' hdfs_dir()
#'
#' # upload an arbitrary file
#' desc <- system.file("DESCRIPTION", package="dplyrXdf")
#' hdfs_upload(desc, "dplyrXdf_description")
#' hdfs_file_exists("dplyrXdf_description")
#'
#' # creates /user/<username>/foo
#' hdfs_dir_create("foo")
#' hdfs_file_copy("dplyrXdf_description", "foo")
#' hdfs_file_exists("foo/dplyrXdf_description")
#'
#' hdfs_file_remove("dplyrXdf_description")
#' hdfs_dir_remove("foo")
#' }
#' @rdname hdfs
#' @export
hdfs_dir <- function(path=".", ..., full_path=FALSE, include_dirs=FALSE, recursive=FALSE,
    dirs_only=FALSE, pattern=NULL, host=hdfs_host())
{
    path <- makeHdfsUri(host, normalizeHdfsPath(path))

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
    #if(!hasUriScheme(path) && substr(path, 1, 1) != "/")
        #message("HDFS user directory assumed to be ", getHdfsUserDir())
    cat("Directory listing of", attr(x, "path"), "\n")
    print.default(c(x), ...)
    invisible(x)
}


#' @param object For \code{in_hdfs} and \code{hdfs_host}, An R object, typically a RevoScaleR data source object.
#'
#' @return
#' \code{hdfs_host} returns the hostname of the HDFS filesystem for the given object. If no object is specified, or if the object is not in HDFS, it returns the hostname of the currently active HDFS filesystem. This is generally "default" unless you are in the \code{RxHadoopMR} or \code{RxSpark} compute context and using an Azure Data Lake Store, in which case it returns the ADLS name node.
#' @rdname hdfs
#' @export
hdfs_host <- function(object=NULL)
{
    if(inherits(object, "RxDataSource"))
        object <- rxGetFileSystem(object)

    if(inherits(object, "RxHdfsFileSystem"))
        return(object$hostName)

    cc <- rxGetComputeContext()
    if(inherits(cc, "RxHadoopMR"))
        return(cc@nameNode)

    fs <- rxGetFileSystem()
    if(inherits(fs, "RxHdfsFileSystem"))
        return(fs$hostName)

    rxGetOption("hdfsHost")
}


#' @details
#' \code{hdfs_dir_exists} and \code{hdfs_file_exists} test for the existence of a given directory and file, respectively. They are analogous to \code{dir.exists} and \code{file.exists} for the native filesystem.
#'
#' @return
#' \code{hdfs_dir_exists} and \code{hdfs_file_exists} return TRUE or FALSE depending on whether the directory or file exists.
#' @rdname hdfs
#' @export
hdfs_dir_exists <- function(path, host=hdfs_host())
{
    detectHdfsConnection()
    path <- makeHdfsUri(host, normalizeHdfsPath(path))
    out <- suppressWarnings(try(hdfs_dir(path, "-d", dirs_only=TRUE), silent=TRUE))
    !inherits(out, "try-error") && length(out) > 0 && out == basename(path)
}


#' @rdname hdfs
#' @export
hdfs_file_exists <- function(path, host=hdfs_host())
{
    detectHdfsConnection()
    path <- makeHdfsUri(host, normalizeHdfsPath(path))
    rxHadoopFileExists(path)
}


#' @details
#' \code{hdfs_dir_create} and \code{hdfs_dir_remove} create and remove directories. They are analogous to \code{dir.create} and \code{unlink(recursive=TRUE)} for the native filesystem.
#'
#' @return
#' The other \code{hdfs_*} functions return TRUE or FALSE depending on whether the operation succeeded.
#' @rdname hdfs
#' @export
hdfs_dir_create <- function(path, ..., host=hdfs_host())
{
    detectHdfsConnection()
    path <- makeHdfsUri(host, normalizeHdfsPaths(path))
    rxHadoopMakeDir(path, ...)
}


#' @rdname hdfs
#' @export
hdfs_dir_remove <- function(path, ..., host=hdfs_host())
{
    detectHdfsConnection()
    path <- makeHdfsUri(host, normalizeHdfsPaths(path))
    rxHadoopRemoveDir(path, ...)
}


#' @details
#' \code{hdfs_file_copy} and \code{hdfs_file_move} copy and move files. They are analogous to \code{file.copy} and \code{file.rename} for the native filesystem. Unlike \code{\link{rxHadoopCopy}} and \code{\link{rxHadoopMove}}, they are vectorised in both \code{src} and \code{dest}.
#'
#' Currently, RevoScaleR has only limited support for accessing multiple HDFS filesystems simultaneously. In particular, \code{src} and \code{dest} should both be on the same HDFS filesystem, whether host or ADLS.
#' @rdname hdfs
#' @export
hdfs_file_copy <- function(src, dest, ..., host=hdfs_host())
{
    detectHdfsConnection()
    src <- makeHdfsUri(host, normalizeHdfsPaths(src))
    dest <- makeHdfsUri(host, normalizeHdfsPaths(dest))
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
hdfs_file_move <- function(src, dest, ..., host=hdfs_host())
{
    detectHdfsConnection()
    src <- makeHdfsUri(host, normalizeHdfsPaths(src))
    dest <- makeHdfsUri(host, normalizeHdfsPaths(dest))
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
hdfs_file_remove <- function(path, ..., host=hdfs_host())
{
    detectHdfsConnection()
    path <- makeHdfsUri(host, normalizeHdfsPaths(path))
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


#' @return
#' \code{in_hdfs} returns whether the given object is stored in HDFS. This will be TRUE for an Xdf data source or file data source in HDFS, or a Spark data source. Classes for the latter include \code{RxHiveData}, \code{RxParquetData} and \code{RxOrcData}.
#' @rdname hdfs
#' @export
in_hdfs <- function(object)
{
    fs <- rxGetFileSystem(object)
    inherits(fs, "RxHdfsFileSystem") || inherits(object, "RxSparkData")
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
#'
#' @examples
#' \dontrun{
#' # set the compute context to Spark
#' rxSparkConnect()
#' local_exec(rxDataStep(mtcars))
#' }
#' @rdname local_exec
#' @export
local_exec <- function(expr, context="local")
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))
    rxSetComputeContext(context)
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
        # check for both mrs-hadoop and hadoop, since the former calls the latter
        if(file.exists(rxGetOption("mrsHadoopPath")) && Sys.which("hadoop") != "")
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
