# paste individual groups back together
combineGroups <- function(datlst, output)
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
                # create (composite) output locally, then copy it
                localXdf <- local_exec(rxDataStep(out, tbl_xdf(fileSystem=RxNativeFileSystem(), createCompositeSet=TRUE),
                                                  rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE)) %>%
                    rename_xdf(basename(output@file))

                out <- copy_to(rxGetFileSystem(output), localXdf, name=dirname(output@file))
                on.exit(delete_xdf(localXdf))
            }
            else out <- rxDataStep(out, output, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE)
        }
    }
    else out <- do.call(rbind.RxXdfData, modify(datlst, .outFile=output))

    out
}




