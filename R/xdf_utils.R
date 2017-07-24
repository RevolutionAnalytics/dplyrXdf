isCompositeXdf <- function(data)
{
    if(!inherits(data, "RxXdfData"))
        return(FALSE)
    cc <- data@createCompositeSet
    if(!is.null(cc))
        return(cc)
    # check if this file refers to an existing directory
    fs <- rxGetFileSystem(data)
    file <- data@file
    if(inherits(fs, "RxNativeFileSystem"))
    {
        return(dir.exists(file) && tools::file_ext(file) == "")
    }
    else
    {
        return(rxHadoopFileExists(file) && tools::file_ext(file) == "")
    }
}


# ensure that composite xdf has no extension, normal xdf has extension
validateXdfFile <- function(filename, composite)
{
    ext <- tools::file_ext(filename)
    if(composite)
    {
        if(ext != "")
            filename <- tools::file_path_sans_ext(filename)
    }
    else
    {
        if(ext != "xdf")
            filename <- paste0(tools::file_path_sans_ext(filename), ".xdf")
    }
    filename
}


# create a new Xdf data source based on an existing Xdf
modifyXdf <- function(xdf, file=xdf@file, varsToKeep=xdf@colNames, varsToDrop=NULL,
    blocksPerRead=xdf@rowsOrBlocksPerRead, fileSystem=xdf@fileSystem,
    createCompositeSet=xdf@createCompositeSet, blocksPerCompositeFile=xdf@blocksPerCompositeFile, ...)
{
    if(is.null(createCompositeSet))
        createCompositeSet <- isCompositeXdf(xdf)
    file <- validateXdfFile(file, createCompositeSet)
    if(varsToKeep == "")
        varsToKeep <- NULL

    RxXdfData(file=file, varsToKeep=varsToKeep, varsToDrop=varsToDrop,
              blocksPerRead=blocksPerRead, fileSystem=fileSystem,
              createCompositeSet=createCompositeSet, blocksPerCompositeFile=blocksPerCompositeFile, ...)
}


# copy an Xdf file via OS commands; avoid rxDataStep
#' @export
copy_xdf <- function(src, dest, overwrite=TRUE)
{
    copyOrMoveXdf(src, dest, overwrite, move=FALSE)
}


# move an Xdf file via OS commands; avoid rxDataStep
#' @export
move_xdf <- function(src, dest, overwrite=TRUE)
{
    copyOrMoveXdf(src, dest, overwrite, move=TRUE)
}


copyOrMoveXdf <- function(src, dest, overwrite=TRUE, move)
{
    if(inherits(dest, "RxFileData"))
        dest <- dest@file

    composite <- isCompositeXdf(src)
    if(in_hdfs(src))
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
            msg <- sprintf("unable to %s to HDFS path %s", if(move) "move" else "copy", path)
            stop(msg, call.=FALSE)
        }
    }
    else
    {
        if(composite)
        {
            renameAfterCopy <- !dir.exists(dest)
            if(renameAfterCopy)
            {
                destName <- validateXdfFile(basename(dest), composite)
                dest <- dirname(dest)
            }
        }
        else
        {
            if(dir.exists(dest))
                dest <- file.path(dest, basename(src@file))
            dest <- validateXdfFile(dest, composite)
        }

        ret <- if(move)
            file.rename(src@file, dest)
        else file.copy(src@file, dest, overwrite=overwrite, recursive=composite)

        if(ret)
        {
            if(composite)
            {
                dest <- file.path(dest, basename(src@file))
                if(renameAfterCopy)
                    modifyXdf(src, file=dest) %>% rename_xdf(destName)
                else modifyXdf(src, file=dest)
            }
            else modifyXdf(src, file=dest)
        }
        else
        {
            msg <- sprintf("unable to %s to path %s", if(move) "move" else "copy", dest)
            stop(msg, call.=FALSE)
        }
    }
}


#' @export
rename_xdf <- function(src, newFile)
{
    if(basename(newFile) != newFile)
        stop("to move an Xdf file to a new location, use move_xdf", call.=FALSE)

    composite <- isCompositeXdf(src)
    if(in_hdfs(src))
    {
        newPath <- file.path(dirname(src@file), newFile, fsep="/")
        rxHadoopMove(src@file, newPath)
        if(composite)
        {
            # rename all files in data and metadata subdirs
            pat <- sprintf("^%s", basename(src@file))
            dataFiles <- hdfs_dir(newPath, full_path=TRUE, recursive=TRUE)
            dataDirs <- dirname(dataFiles)
            newDataFiles <- file.path(dataDirs, sub(pat, newFile, basename(dataFiles)))
            mapply(rxHadoopMove, dataFiles, newDataFiles)
        }
    }
    else
    {
        newPath <- file.path(dirname(src@file), newFile)
        file.rename(src@file, newPath)
        if(composite)
        {
            # rename all files in data and metadata subdirs
            pat <- sprintf("^%s", basename(src@file))
            dataFiles <- dir(newPath, pattern=pat, full.names=TRUE, recursive=TRUE)
            dataDirs <- dirname(dataFiles)
            newDataFiles <- file.path(dataDirs, sub(pat, newFile, basename(dataFiles)))
            file.rename(dataFiles, newDataFiles)
        }
    }
    modifyXdf(src, file=newPath)
}


#' @export
delete_xdf <- function(xdf)
{
    if(in_hdfs(xdf))
    {
        if(isCompositeXdf(xdf))
            rxHadoopRemoveDir(xdf@file)
        else rxHadoopRemove(xdf@file)
    }
    else unlink(xdf@file, recursive=TRUE)
}

