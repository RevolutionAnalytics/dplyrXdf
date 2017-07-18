#' @export
copy_to.RxHdfsFileSystem <- function(dest, df, path=NULL, overwrite=FALSE, force_composite=TRUE, ...)
{
    isRemoteHdfsClient()  # fail early if no HDFS found

    if(is.character(df))
        df <- RxXdfData(df)

    if(inherits(df, "RxXdfData"))
    {
        if(isHdfs(df))
            stop("source is already in HDFS")
        localName <- df@file
    }
    else # if not an Xdf, create an Xdf
    {
        if(!is.data.frame(df) && !inherits(df, "RxDataSource"))
        {
            # try converting to local data frame
            df <- try(as.data.frame(df))
            if(inherits(df, "try-error"))
                stop("unable to import source")
        }

        localName <- if(!is.null(name))
            file.path(get_dplyrxdf_dir("native"), basename(name))
        else file.path(get_dplyrxdf_dir("native"), deparse(substitute(df)))

        df <- execOnHdfsClient(rxDataStep(df,
            tbl_xdf(file=localName, fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE),
            rowsPerRead=.dxOptions$rowsPerRead))
    }

    if(force_composite && !isCompositeXdf(df))
    {
        # create a composite copy of non-composite src
        # this happens on client if remote
        message("Creating composite copy of non-composite Xdf ", df@file)

        localName <- file.path(get_dplyrxdf_dir("native"), basename(df@file))
        df <- execOnHdfsClient(rxDataStep(df,
            tbl_xdf(file=localName, fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE),
            rowsPerRead=.dxOptions$rowsPerRead))
    }

    if(is.null(path))
        path <- getHdfsUserDir()

    hdfsCopyBase(df@file, path, overwrite=overwrite, isDir=isCompositeXdf(df), ...)

    RxXdfData(file.path(path, basename(df@file), fsep="/"), fileSystem=dest, createCompositeSet=isCompositeXdf(df))
}


hdfsCopyBase <- function(src, dest, nativeTarget="/tmp", overwrite, isDir, ...)
{
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






