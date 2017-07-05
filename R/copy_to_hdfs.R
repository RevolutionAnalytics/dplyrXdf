#' @export
copy_to.RxHadoopMR <- function(dest, df, name=NULL, overwrite=FALSE, force_composite=FALSE, ...)
{
    isRemoteHdfsClient()  # fail early if no HDFS found

    if(inherits(df, "RxFileData"))
    {
        if(isHdfs(rxGetFileSystem(df)))
            stop("source is already in HDFS")
        src <- df
    }
    else if(is.character(df))
        src <- RxXdfData(df)
    else # write to a file, then copy the file
    {
        if(!is.data.frame(df) && !inherits(df, "RxDataSource"))
        {
            # try converting to local data frame
            df <- try(as.data.frame(df))
            if(inherits(df, "try-error"))
                stop("unable to import source")
        }
        cc <- rxGetComputeContext()
        rxSetComputeContext("local")
        src <- rxDataStep(df, tbl_xdf(fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE))
        rxSetComputeContext(cc)
    }

    if(is.null(name))
        name <- basename(src@file)

    hdfsCopyBase(src@file, name, overwrite=overwrite, ...)
}


#' @export
copy_to.RxHdfsFileSystem <- copy_to.RxHadoopMR


## copies data sources to/from HDFS
#' @export
copy_to.RxFileData <- function(dest, df, name=deparse(substitute(df)), overwrite=FALSE, ...)
{

}


#copy_to(hdfs, "/misc/mtcarsc") --> ./mtcarsc
#copy_to(hdfs, xdf, "/foo/bar") --> /foo/bar/mtcarsc
#RevoScaleR:::rxRemoteCopy(sp, "misc/mtcarsc", FALSE, "/tmp/mtcarsc", TRUE, extraSwitches="-r")
#rxHadoopCopyFromLocal("/tmp/mtcarsc", "/user/sshuser")

#mth <- RxXdfData("/user/sshuser/mtcarsc", fileSystem=hd, createCompositeSet=TRUE)

hdfsCopyBase <- function(src, dest, nativeTarget="/tmp", overwrite=TRUE)
{
    # based on rxHadoopCopyFromClient
    if(isRemoteHdfsClient())
    {
        nativeTarget <- file.path(nativeTarget, basename(src), fsep="/")
        isDir <- file.info(src)$isdir
        extraSwitches <- if(isDir) "-r" else ""
        RevoScaleR:::rxRemoteCopy(rxGetComputeContext(), src, FALSE, nativeTarget, TRUE, extraSwitches)
        src <- nativeTarget
    }
    rxHadoopCopyFromLocal(src, dest)
}

