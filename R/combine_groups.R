# paste individual groups back together
combineGroups <- function(datlst, output, grps)
{
    if(missing(output))
        output <- tbl_xdf(datlst[[1]])

    if(is.data.frame(datlst[[1]]))
    {
        out <- bind_rows(datlst)
        if(!is.null(output))
            out <- rxDataStep(out, output, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE)
    }
    else out <- do.call(rbind.RxXdfData, c(datlst, .outFile=output))

    simpleRegroup(out, grps)
}




