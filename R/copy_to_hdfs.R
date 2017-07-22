#' @export
copy_to.RxHdfsFileSystem <- function(dest, df, path=NULL, overwrite=FALSE, force_composite=TRUE, ...)
{
    isRemoteHdfsClient()  # fail early if no HDFS found

    if(is.character(df))
        df <- RxXdfData(df)

    if(inherits(df, "RxXdfData"))
    {
        if(in_hdfs(df))
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

        df <- execOnHdfsClient(rxDataStep(df,
            tbl_xdf(fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE),
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

    hdfsUpload(df@file, path, overwrite=overwrite, isDir=isCompositeXdf(df), ...)

    RxXdfData(file.path(path, basename(df@file), fsep="/"), fileSystem=dest, createCompositeSet=isCompositeXdf(df))
}


hdfsUpload <- function(src, dest, nativeTarget="/tmp", overwrite, isDir, ...)
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


hdfsDownload <- function(src, dest, nativeTarget="/tmp", overwrite, isDir, ...)
{
    localDest <- if(isRemoteHdfsClient()) nativeTarget else dest

    cmd <- "fs -copyToLocal"
    if(!missing(overwrite))
        warning("overwrite option not supported for HDFS downloading")
    cmd <- paste(cmd, src, localDest)

    ret <- rxHadoopCommand(cmd, ...)

    if(ret && isRemoteHdfsClient())
    {
        if(isDir)
        {
            localDest <- file.path(localDest, basename(src))
            #dest <- file.path(dest, basename(src))
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

