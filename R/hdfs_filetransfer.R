#' Transfer files and directories to and from HDFS
#'
#' @param src,dest Character strings giving the source and destination paths.
#' @param overwrite Whether to overwrite existing files at the destination.
#' @param nativeTarget Only when transferring to/from a remote client. The directory on the edge node in which to stage files.
#' @param ... Other arguments to the Hadoop \code{copyFromLocal}/\code{copyToLocal} command.
#'
#' @details
#' These functions transfer files and directories between the native filesystem and HDFS. \code{hdfs_upload} copies files from the native filesystem into HDFS, and \code{hdfs_download} does the reverse. They can be used both from the edge node of a Hadoop/Spark cluster, and from a remote client. In the latter case, the transfer is a two-stage process: for downloading, the files are copied to the edge node in the directory given by \code{nativeTarget}, and then copied to the client; and vice-versa for uploading.
#'
#' Note that renaming directories as part of the transfer is supported for downloading from HDFS, but not for uploading.
#'
#' @return
#' A logical value indicating whether the file transfer succeeded.
#'
#' @seealso
#' \code{\link{download.file}}, \code{\link{rxHadoopCopyFromLocal}}, \code{\link{rxHadoopCopyFromClient}}
#' @rdname hdfs_filetransfer
#' @export
hdfs_upload <- function(src, dest, overwrite=TRUE, nativeTarget="/tmp", ...)
{
    detectHdfsConnection()
    isDir <- dir.exists(src)

    if(!overwrite)
    {
        exists <- if(isDir)
            hdfs_dir_exists(file.path(dest, basename(src), fsep="/"))
        else
        {
            isDestDir <- hdfs_dir_exists(dest)
            if(isDestDir)
                hdfs_file_exists(file.path(dest, basename(src), fsep="/"))
            else hdfs_file_exists(dest)
        }
        if(exists)
            stop(sprintf("target %s exists; set overwrite=TRUE to replace it",
                if(isDir) "directory" else "file"))
    }

    # based on rxHadoopCopyFromClient
    if(isRemoteHdfsClient())
    {
        ## need to add name for directory, but not for file
        #rxRemoteCopy(cc, "file.xdf", FALSE, "/tmp", TRUE, extraSwitches="")
        #rxRemoteCopy(cc, "dir", FALSE, "/tmp/dir", TRUE, extraSwitches="-r")

        if(isDir)
            nativeTarget <- file.path(nativeTarget, basename(src), fsep="/")
        extraSwitches <- if(isDir) "-r" else ""

        RevoScaleR:::rxRemoteCopy(rxGetComputeContext(), shQuote(src), FALSE, nativeTarget, TRUE, extraSwitches)

        if(isDir)
            src <- nativeTarget
        else src <- file.path(nativeTarget, basename(src), fsep="/")
    }

    if(isDir)
        dest <- file.path(dest, basename(src), fsep="/")

    # based on rxHadoopCopyFromLocal, with -f option for overwriting
    cmd <- "fs -copyFromLocal"
    if(overwrite)
        cmd <- paste(cmd, "-f")
    cmd <- paste(cmd, src, dest)

    ret <- rxHadoopCommand(cmd, ...)
    if(ret)
    {
        cmd <- paste0("sudo rm -rf ", src)
        RevoScaleR:::rxRemoteCommand(rxGetComputeContext(), cmd)
    }
    ret
}


#' @rdname hdfs_filetransfer
#' @export
hdfs_download <- function(src, dest, overwrite=TRUE, nativeTarget="/tmp", ...)
{
    detectHdfsConnection()
    isDir <- hdfs_dir_exists(src)

    if(!overwrite)
    {
        exists <- if(isDir)
            dir.exists(file.path(dest, basename(src)))
        else
        {
            isDestDir <- hdfs_dir_exists(dest)
            if(isDestDir)
                file.exists(file.path(dest, basename(src)))
            else file.exists(dest)
        }
        if(exists)
            stop(sprintf("target %s exists; set overwrite=TRUE to replace it",
                if(isDir) "directory" else "file"))
    }

    localDest <- if(isRemoteHdfsClient()) nativeTarget else dest

    cmd <- paste("fs -copyToLocal", src, localDest)
    ret <- rxHadoopCommand(cmd, ...)

    if(ret && isRemoteHdfsClient())
    {
        if(isDir)
        {
            localDest <- file.path(localDest, basename(src))
            extraSwitches <- "-r"
        }
        else
        {
            localDest <- file.path(localDest, basename(src), fsep="/")
            extraSwitches <- ""
        }
        RevoScaleR:::rxRemoteCopy(rxGetComputeContext(), localDest, TRUE, shQuote(dest), FALSE, extraSwitches)

        cmd <- paste0("sudo rm -rf ", localDest)
        RevoScaleR:::rxRemoteCommand(rxGetComputeContext(), cmd)
    }
    else ret
}

