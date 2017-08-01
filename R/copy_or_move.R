copyOrMove <- function(src, dest, overwrite=TRUE, move)
{
    if(inherits(dest, "RxFileData"))
        dest <- dest@file

    composite <- isCompositeXdf(src)

    if(in_hdfs(src))
        copyOrMoveHdfs(src, dest, overwrite=overwrite, move=move, composite=composite)
    else copyOrMoveNative(src, dest, overwrite=overwrite, move=move, composite=composite)
}


copyOrMoveHdfs <- function(src, dest, overwrite, move, composite)
{
    if(composite)
    {
        renameAfterCopy <- !hdfs_dir_exists(dest)
        if(renameAfterCopy)
        {
            destName <- validateXdfFile(basename(dest), composite)
            dest <- dirname(dest)
        }
    }
    else
    {
        # test that directory exists
        cmd <- paste0("fs -test -d '", dest, "'")
        if(rxHadoopCommand(cmd))
            dest <- file.path(dest, basename(src@file), fsep="/")
        dest <- validateXdfFile(dest, composite)
    }

    # do we actually need to copy/move?
    if(normalizeHdfsPath(dirname(src@file)) == normalizeHdfsPath(dest))
        return(rename_xdf(src, basename(dest)))

    ret <- if(move)
        rxHadoopMove(src@file, dest)
    else rxHadoopCopy(src@file, dest)

    if(ret)
    {
        if(composite)
        {
            dest <- file.path(dest, basename(src@file), fsep="/")
            if(renameAfterCopy)
                modifyXdf(src, file=dest) %>% rename_xdf(destName)
            else modifyXdf(src, file=dest)
        }
        else modifyXdf(src, file=dest)
    }
    else
    {
        msg <- sprintf("unable to %s to HDFS path %s", if(move) "move" else "copy", dest)
        stop(msg, call.=FALSE)
    }
}


copyOrMoveNative <- function(src, dest, overwrite, move, composite)
{
    .path <- function(p)
    normalizePath(p, mustWork=FALSE)

    # why are directories so hard
    if(composite)
    {
        srcDir <- dirname(src@file)
        destDir <- dirname(dest)
        srcFile <- basename(src@file)
        destFile <- basename(dest)

        sameDir <- .path(srcDir) == .path(destDir)
        sameFile <- srcFile == destFile
        if(sameDir && sameFile)
            stop("source and destination are the same", call.=FALSE)

        # make an xdf under the given dest, or at the given dest?
        underDest <- dir.exists(dest)
        renameAfterCopy <- !underDest

        if(sameDir && !underDest)
        {
            # is it just a rename op?
            if(move)
                return(rename_xdf(src, basename(dest)))

            # otherwise copy individual files/dirs within src
            dir.create(dest)
            srcData <- list.dirs(src@file, recursive=FALSE)
            ret <- all(file.copy(srcData, dest, recursive=TRUE))
        }
        else
        {
            if(!underDest)
            {
                # making an xdf underneath given dir (will have same name)
                dir.create(dest)
                srcData <- list.dirs(src@file, recursive=FALSE)
                ret <- all(file.copy(srcData, dest, recursive=TRUE))
            }
            else
            {
                # making an xdf at given dir
                ret <- if(move)
                    moveDir(src@file, file.path(dest, srcFile))
                else file.copy(src@file, dest, overwrite=overwrite, recursive=composite)
            }
        }

        if(ret)
        {
            if(renameAfterCopy)
            {
                pat <- sprintf("^%s", srcFile)
                dataFiles <- dir(dest, pattern=pat, full.names=TRUE, recursive=TRUE)
                dataDirs <- dirname(dataFiles)
                newDataFiles <- file.path(dataDirs, sub(pat, destFile, basename(dataFiles)))
                file.rename(dataFiles, newDataFiles)
            }
            else dest <- file.path(dest, srcFile)

            out <- modifyXdf(src, file=dest)
        }
    }
    else
    {
        if(dir.exists(dest))
            dest <- file.path(dest, basename(src@file))
        dest <- validateXdfFile(dest, composite)

        ret <- if(move)
            file.rename(src@file, dest)
        else file.copy(src@file, dest, overwrite=overwrite, recursive=composite)

        if(ret)
            out <- modifyXdf(src, file=dest)
    }
    if(!ret)
    {
        msg <- sprintf("unable to %s to path %s", if(move) "move" else "copy", dest)
        stop(msg, call.=FALSE)
    }
    out
}


# R has platform-specific limitations moving directories -- do it file by file
moveDir <- function(src, dest)
{
    srcDirs <- list.dirs(src, recursive=TRUE, full.names=FALSE)
    destDirs <- file.path(dest, srcDirs)
    lapply(destDirs, dir.create)

    files <- list.files(src, recursive=TRUE)
    srcFiles <- file.path(src, files)
    destFiles <- file.path(dest, files)

    ret <- all(file.rename(srcFiles, destFiles))

    if(ret)
        unlink(src, recursive=TRUE)
    else stop("unable to move directory ", src)
    ret
}
