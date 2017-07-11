#' @export
copy_to.RxHdfsFileSystem <- function(dest, df, name=NULL, overwrite=FALSE, force_composite=FALSE, ...)
{
    isRemoteHdfsClient()  # fail early if no HDFS found

    if(is.character(df))
        src <- RxXdfData(df)

    if(inherits(df, "RxXdfData"))
    {
        if(isHdfs(rxGetFileSystem(df)))
            stop("source is already in HDFS")
        src <- df
        if(!is.null(name))
            warning("renaming Xdf file on copy not yet supported")
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

        cc <- rxGetComputeContext()
        rxSetComputeContext("local")
        src <- rxDataStep(df,
            tbl_xdf(file=localName, fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE),
            rowsPerRead=.dxOptions$rowsPerRead)
        rxSetComputeContext(cc)
    }

    if(force_composite && !isCompositeXdf(src))
    {
        # create a composite copy of non-composite src
        # this happens on client if remote
        message("Creating composite copy of non-composite Xdf ", src@file)

        cc <- rxGetComputeContext()
        rxSetComputeContext("local")
        localName <- file.path(get_dplyrxdf_dir("native"), basename(src@file))
        src <- rxDataStep(src,
            tbl_xdf(file=localName, fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE),
            rowsPerRead=.dxOptions$rowsPerRead)
        rxSetComputeContext(cc)
    }

    # ensure Xdf composite filenames are correct
    name <- basename(src@file)

    hdfsCopyBase(src@file, name, overwrite=overwrite, ...)
    RxXdfData(name, fileSystem=dest)
}


hdfsCopyBase <- function(src, dest, nativeTarget="/tmp", overwrite, ...)
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

