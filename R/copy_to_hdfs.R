#' @export
copy_to.RxHadoopMR <- function(dest, df, name=NULL, overwrite=FALSE, force_composite=FALSE, ...)
{
    isRemoteHdfsClient()  # fail early if no HDFS found

    if(inherits(df, "RxXdfData"))
    {
        if(isHdfs(rxGetFileSystem(df)))
            stop("source is already in HDFS")
        src <- df
        if(!is.null(name))
            warning("renaming Xdf file on copy not yet supported")
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

        localName <- if(!is.null(name))
            validateXdfFile(file.path(get_dplyrxdf_dir("native"), basename(name)), composite=TRUE)
        else NULL

        src <- rxDataStep(df, tbl_xdf(file=localName, fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE))
        print(src)
        rxSetComputeContext(cc)
    }

    # ensure Xdf composite filenames are correct
    name <- basename(src@file)

    hdfsCopyBase(src@file, name, overwrite=overwrite, ...)
    RxXdfData(name, fileSystem=RxHdfsFileSystem())
}


#' @export
copy_to.RxHdfsFileSystem <- copy_to.RxHadoopMR


hdfsCopyBase <- function(src, dest, nativeTarget="/tmp", overwrite=FALSE, ...)
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
    rxHadoopCopyFromLocal(src, dest, ...)
}

