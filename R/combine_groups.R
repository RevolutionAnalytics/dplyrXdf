# paste individual groups back together
combineGroups <- function(datlst, output, grps)
{
    if(missing(output))
        output <- tbl_xdf(datlst[[1]])

    if(is.data.frame(datlst[[1]]))
    {
        out <- bind_rows(datlst)
        if(!is.null(output))
        {
            if(in_hdfs(output) && inherits(rxGetComputeContext(), "RxHadoopMR"))
            {
                # create output locally, then copy it
                localFile <- file.path(get_dplyrxdf_dir(RxNativeFileSystem()), basename(output@file))
                localXdf <- execOnHdfsClient(rxDataStep(out, localFile, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE))

                out <- copy_to(rxGetFileSystem(output), localXdf, path=dirname(output))
            }
            else out <- rxDataStep(out, output, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE)
        }
    }
    else out <- do.call(rbind.RxXdfData, rlang::modify(datlst, .outFile=output))

    out
}




