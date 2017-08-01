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
    if(identical(varsToKeep, ""))
        varsToKeep <- NULL

    RxXdfData(file=file, varsToKeep=varsToKeep, varsToDrop=varsToDrop,
              blocksPerRead=blocksPerRead, fileSystem=fileSystem,
              createCompositeSet=createCompositeSet, blocksPerCompositeFile=blocksPerCompositeFile, ...)
}


# copy an Xdf file via OS commands; avoid rxDataStep
#' @export
copy_xdf <- function(src, dest, overwrite=TRUE)
{
    copyOrMove(src, dest, overwrite, move=FALSE)
}


# move an Xdf file via OS commands; avoid rxDataStep
#' @export
move_xdf <- function(src, dest, overwrite=TRUE)
{
    copyOrMove(src, dest, overwrite, move=TRUE)
}


#' @export
rename_xdf <- function(src, newFile)
{
    if(dirname(newFile) == dirname(src@file))
        newFile <- basename(newFile)
    else if(basename(newFile) != newFile)
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
    else if(inherits(xdf, "RxXdfData"))
        unlink(xdf@file, recursive=TRUE)
}

