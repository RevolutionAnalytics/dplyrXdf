copyOrMove <- function(src, dest, overwrite=TRUE, move)
{
    if(inherits(dest, "RxFileData"))
        dest <- dest@file

    composite <- is_composite_xdf(src)

    if(in_hdfs(src))
        copyOrMoveHdfs(src, dest, overwrite=overwrite, move=move, composite=composite)
    else copyOrMoveNative(src, dest, overwrite=overwrite, move=move, composite=composite)
}


copyOrMoveHdfs <- function(src, dest, overwrite, move, composite)
{
    host <- src@fileSystem$hostName
    if(composite)
    {
        srcDir <- dirname(src@file)
        destDir <- dirname(dest)
        srcFile <- basename(src@file)
        destFile <- basename(dest)

        sameDir <- normalizeHdfsPath(srcDir) == normalizeHdfsPath(destDir)
        sameFile <- srcFile == destFile
        if(sameDir && sameFile)
            stop("source and destination are the same", call.=FALSE)

        # make an xdf under the given dest, or at the given dest?
        underDest <- hdfs_dir_exists(dest)
        renameAfterCopy <- !underDest

        if(sameDir && !underDest)
        {
            # is it just a rename op?
            if(move)
                return(rename_xdf(src, basename(dest)))

            # otherwise copy individual files/dirs within src
            ret <- hdfs_file_copy(src@file, dest, host=host)
        }
        else
        {
            if(!underDest)
            {
                # making an xdf at given dir
                dest <- validateXdfFile(dest, composite)
                ret <- if(move)
                    hdfs_file_move(src@file, dest, host=host)
                else hdfs_file_copy(src@file, dest, host=host)
            }
            else
            {
                # making an xdf underneath given dir (will have same name)
                ret <- if(move)
                    hdfs_file_move(src@file, file.path(dest, srcFile), host=host)
                else hdfs_file_copy(src@file, dest, host=host)
            }
        }

        if(ret)
        {
            if(renameAfterCopy)
            {
                pat <- paste0("^", srcFile)
                dataFiles <- hdfs_dir(dest, full_path=TRUE, recursive=TRUE, host=host)
                dataDirs <- dirname(dataFiles)
                newDataFiles <- file.path(dataDirs, sub(pat, destFile, basename(dataFiles)))
                ret <- all(mapply(hdfs_file_move, dataFiles, newDataFiles, MoreArgs=list(host=host)))
            }
            else dest <- file.path(dest, srcFile)

            out <- modifyXdf(src, file=dest)
        }
    }
    else stop("only composite Xdf files supported in HDFS")
    #{
        #if(dir.exists(dest))
            #dest <- file.path(dest, basename(src@file))
        #dest <- validateXdfFile(dest, composite)

        #ret <- if(move)
            #file.rename(src@file, dest)
        #else rxHadoopCopy(src@file, dest, overwrite=overwrite, recursive=composite)

        #if(ret)
            #out <- modifyXdf(src, file=dest)
    #}

    if(!ret)
    {
        msg <- sprintf("unable to %s to HDFS path %s", if(move) "move" else "copy", dest)
        stop(msg, call.=FALSE)
    }
    out
}


copyOrMoveNative <- function(src, dest, overwrite, move, composite)
{
    # why are directories so hard
    if(composite)
    {
        srcDir <- dirname(src@file)
        destDir <- dirname(dest)
        srcFile <- basename(src@file)
        destFile <- basename(dest)

        sameDir <- normalizePath(srcDir, mustWork=FALSE) == normalizePath(destDir, mustWork=FALSE)
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
                return(rename_xdf(src, destFile))

            # otherwise copy individual files/dirs within src
            dir.create(dest)
            srcData <- list.dirs(src@file, recursive=FALSE)
            ret <- all(file.copy(srcData, dest, recursive=TRUE))
        }
        else
        {
            if(!underDest)
            {
                # making an xdf at given dir
                dest <- validateXdfFile(dest, composite)
                ret <- if(move)
                    moveDir(src@file, dest)
                else
                {
                    dir.create(dest)
                    srcData <- list.dirs(src@file, recursive=FALSE)
                    all(file.copy(srcData, dest, recursive=TRUE))
                }
            }
            else
            {
                # making an xdf underneath given dir (will have same name)
                ret <- if(move)
                    moveDir(src@file, file.path(dest, srcFile))
                else file.copy(src@file, dest, overwrite=overwrite, recursive=composite)
            }
        }

        if(ret)
        {
            if(renameAfterCopy)
            {
                pat <- paste0("^", srcFile)
                dataFiles <- dir(dest, full.names=TRUE, recursive=TRUE)
                dataDirs <- dirname(dataFiles)
                newDataFiles <- file.path(dataDirs, sub(pat, destFile, basename(dataFiles)))
                ret <- all(file.rename(dataFiles, newDataFiles))
            }
            else dest <- file.path(dest, srcFile)

            out <- modifyXdf(src, file=dest)
        }
    }
    else  # noncomposite = regular file
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
    ret
}
