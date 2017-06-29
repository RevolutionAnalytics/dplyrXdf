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


copyXdf <- function(from, to, move=FALSE)
{
    if(inherits(from, "RxXdfData"))
        from <- rxXdfFileName(from)
    from <- normalizePath(from)
    fname <- basename(from)
    to <- normalizePath(file.path(to, fname))
    if(move)
        file.rename(from, to)
    else file.copy(from, to)
    RxXdfData(to)
}


