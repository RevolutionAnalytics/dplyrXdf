# paste individual groups back together
combineGroups <- function(datlst, output, grps)
{
    out <- if(inherits(datlst[[1]], "data.frame"))
        combineGroupDfs(datlst, output)
    else combineGroupXdfs(datlst, output)
    simpleRegroup(out, grps)
}


# paste individual group xdfs back together
combineGroupXdfs <- function(xdflst, output, grps)
{
    on.exit(deleteIfTbl(xdflst))
    xdf1 <- xdflst[[1]]

    dropVars <- if(".group." %in% names(xdf1)) ".group." else NULL

    if(missing(output)) # xdf tbl
        output <- tbl_xdf()

    out <- rxDataStep(xdf1, output, varsToDrop=dropVars, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE)
    # use rxDataStep loop for appending instead of rxMerge; latter is surprisingly slow
    stopIfHdfs("combineGroup not supported on HDFS")  # should never trip this
    for(xdf in xdflst[-1])
        out <- rxDataStep(xdf, output, varsToDrop=dropVars, append="rows", computeLowHigh=FALSE)
    out
}


# paste individual group data frames back together
combineGroupDfs <- function(dflst, output)
{
    if(missing(output))
        output <- tbl_xdf()

    if(is.null(output))
        bind_rows(dflst)
    else rxDataStep(bind_rows(dflst), output, rowsPerRead=.dxOptions$rowsPerRead, overwrite=TRUE)
}


