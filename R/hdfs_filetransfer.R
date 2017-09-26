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
#'
#' @examples
#' \dontrun{
#' as_xdf(mtcars, "mtcars.xdf", overwrite=TRUE)
#' hdfs_upload("mtcars.xdf", ".")
#'
#' write.csv(mtcars, "mtcars.csv", row.names=FALSE)
#' hdfs_upload("mtcars.csv", "mtcars_uploaded.csv")
#'
#' file.remove("mtcars.csv")
#' hdfs_download("mtcars_uploaded.csv", "mtcars.csv")
#' read.csv("mtcars.csv")
#'
#' # hdfs_upload() and hdfs_download() can transfer any file, not just datasets
#' desc <- system.file("DESCRIPTION", package="dplyrXdf")
#' hdfs_upload(desc, "dplyrXdf_description")
#'
#' # uploading to attached ADLS storage
#' hdfs_upload("mtcars.xdf", ".", host="adls.host.name")
#' }
#' @rdname hdfs_filetransfer
#' @export
hdfs_upload <- function(src, dest, overwrite=FALSE, nativeTarget="/tmp", host=hdfs_host(), ...)
{
    detectHdfsConnection()
    isDir <- dir.exists(src)

    destExists <- if(isDir)
        hdfs_dir_exists(file.path(dest, basename(src), fsep="/"), host)
    else
    {
        isDestDir <- hdfs_dir_exists(dest, host)
        if(isDestDir)
            hdfs_file_exists(file.path(dest, basename(src), fsep="/"), host)
        else hdfs_file_exists(dest, host)
    }
    if(destExists)
    {
        if(overwrite)
        {
            # copyFromLocal can overwrite files
            if(isDir)
                hdfs_dir_remove(file.path(dest, basename(src), fsep="/"), host=host)
        }
        else stop(sprintf("destination %s exists; set overwrite=TRUE to replace it",
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

        res <- remoteCopy(rxGetComputeContext(), shQuote(src), FALSE, nativeTarget, TRUE, extraSwitches)

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
    cmd <- paste(cmd, src, makeHdfsUri(host, normalizeHdfsPath(dest)))

    ret <- rxHadoopCommand(cmd, ...)
    if(ret && isRemoteHdfsClient())
    {
        cmd <- paste0("sudo rm -rf ", src)
        RevoScaleR:::rxRemoteCommand(rxGetComputeContext(), cmd)
    }
    ret
}


#' @rdname hdfs_filetransfer
#' @export
hdfs_download <- function(src, dest, overwrite=FALSE, nativeTarget="/tmp", host=hdfs_host(), ...)
{
    detectHdfsConnection()
    isDir <- hdfs_dir_exists(src, host)

    destExists <- if(isDir)
        dir.exists(file.path(dest, basename(src)))
    else
    {
        isDestDir <- dir.exists(dest)
        if(isDestDir)
            file.exists(file.path(dest, basename(src)))
        else file.exists(dest)
    }
    if(destExists)
    {
        if(overwrite)
        {
            if(isDir || isDestDir)
                unlink(file.path(dest, basename(src)), recursive=TRUE)
            else unlink(dest)
        }
        else stop(sprintf("destination %s exists; set overwrite=TRUE to replace it",
                          if(isDir) "directory" else "file"))
    }

    localDest <- if(isRemoteHdfsClient()) nativeTarget else dest

    cmd <- paste("fs -copyToLocal", makeHdfsUri(host, normalizeHdfsPath(src)), localDest)
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
        remoteCopy(rxGetComputeContext(), localDest, TRUE, shQuote(dest), FALSE, extraSwitches)

        cmd <- paste0("sudo rm -rf ", localDest)
        RevoScaleR:::rxRemoteCommand(rxGetComputeContext(), cmd)
    }
    else ret
}


# hacks to fix RevoScaleR:::rxRemoteCopy and rxScp
remoteCopy <- function(computeContext, source, sourceIsRemote, target, targetIsRemote, extraSwitches="")
{
    localCopyOnly <- computeContext@onClusterNode
    if(localCopyOnly)
    {
        rxLocalCopy(source, target)
    }
    else
    {
        userAtHost <- paste(computeContext@sshUsername, "@", computeContext@sshHostname, sep="")

        if(sourceIsRemote && !localCopyOnly)
        {
            source <- paste(userAtHost, ":", source, sep="")
        }
        if(!sourceIsRemote && !localCopyOnly)
        {
            target <- paste(userAtHost, ":", target, sep="")
        }
        return(scp(source, target,
                   switches=paste(computeContext@sshSwitches, extraSwitches),
                   profileScript=computeContext@sshProfileScript,
                   sshStrictHostKeyChecking=computeContext@sshStrictHostKeyChecking,
                   sshClientDir=computeContext@sshClientDir))
    }

}


scp <- function(source, target, switches="", profileScript=NULL, sshStrictHostKeyChecking="ask", sshClientDir="")
{
    scriptOutput <- NULL
    printOut <- NULL
    havePreformedScp <- FALSE
    userAtHost <- NULL
    if(.Platform$OS.type == "windows")
    {
        sshClient <- RevoScaleR:::rxFindWindowsSsh("pscp.exe", "scp.exe", sshClientDir)
        command <- sshClient$path

        # don't scribble on the switches
        #switches <- sub("-p", "-P , switches, fixed=TRUE)

        if(sshClient$usePutty)
        {
            if(grepl("@", target, fixed=TRUE))
            {
                userAtHostAndTargetDir <- unlist(strsplit(target, ":", fixed=TRUE))
                userAtHost <- userAtHostAndTargetDir[[1]]

                targetDir <- userAtHostAndTargetDir[[2]]
                cmd <- paste("mkdir -p ", targetDir)

                RevoScaleR:::rxSsh(userAtHost, cmd, sub("-r", "", switches, fixed=TRUE),
                                   profileScript=profileScript,
                                   sshStrictHostKeyChecking=sshStrictHostKeyChecking,
                                   sshClientDir=sshClientDir)

                if(grepl("-r", switches, fixed=TRUE) && (substring(source, nchar(source)) != "\\"))
                {
                    source <- paste(source, "\\", sep="")
                }
            }
            scriptOutput <- capture.output(suppressWarnings(
                printOut <- system2(command, paste(switches, "-q", source, target),
                                    stdout=TRUE, stderr=TRUE, input="")
            ))
            havePreformedScp <- TRUE
        }
        else
        {
            source <- RevoScaleR:::rxuFixForCygwinIfLocal(source)
            target <- RevoScaleR:::rxuFixForCygwinIfLocal(target)
        }
    }
    else command <- "scp"

    if(!havePreformedScp)
    {
        args <- paste(paste0("-o StrictHostKeyChecking=", sshStrictHostKeyChecking),
                      switches, "-q", source, target)
        scriptOutput <- capture.output(suppressWarnings(
            printOut <- system2(command, args=args, input="", stdout=TRUE, stderr=TRUE)
        ))
    }
    RevoScaleR:::checkSshReturnMessage(printOut)
    RevoScaleR:::printSecureOutput(scriptOutput, userAtHost)
}
