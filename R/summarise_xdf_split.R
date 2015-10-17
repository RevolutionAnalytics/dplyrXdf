#' @include summarise_xdf.R
#' @include summarise_xdf_summary.R
NULL


smry_rxSplit <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(is.null(grps))
        return(smry_rxSummary(data, grps, stats, exprs, rxArgs))

    outFile <- tblFile(data)
    xdflst <- split_groups(data, outFile)
    xdflst <- rxExec(smry_rxSummary_with_groupvars, data=rxElemArg(xdflst), grps, stats, exprs, rxArgs, packagesToLoad="dplyrXdf")
    combine_groups(xdflst, outFile, NULL)
}


smry_rxSummary_with_groupvars <- function(data, grps, stats, exprs, rxArgs)
{
    gvars <- rxReadXdf(data, varsToKeep=grps, numRows=1)
    smry <- smry_rxSummary(data, NULL, stats, exprs, rxArgs, dfOut=TRUE)
    rxDataFrameToXdf(cbind(gvars, smry, stringsAsFactors=FALSE), rxXdfFileName(data), overwrite=TRUE)
}


smry_rxSplit_dplyr <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(is.null(grps))
    {
        invars <- invars(exprs)
        cl <- quote(rxXdfToDataFrame(data))
        cl[names(rxArgs)] <- rxArgs
        return(eval(cl) %>%
               dplyr::summarise_(.dots=exprs) %>%
               rxDataFrameToXdf(tblFile(.), overwrite=TRUE))
    }

    outFile <- tblFile(data)
    xdflst <- split_groups(data, outFile)
    xdflst <- rxExec(smry_dplyr_with_groupvars, data=rxElemArg(xdflst), grps, exprs, rxArgs, packagesToLoad="dplyrXdf")
    combine_groups(xdflst, outFile, NULL)
}


smry_dplyr_with_groupvars <- function(data, grps, exprs, rxArgs)
{
    invars <- invars(exprs)
    gvars <- rxReadXdf(data, varsToKeep=grps, numRows=1)
    cl <- quote(rxXdfToDataFrame(data))
    cl[names(rxArgs)] <- rxArgs
    smry <- eval(cl) %>%
        dplyr::summarise_(.dots=exprs)
        
    rxDataFrameToXdf(cbind(gvars, smry, stringsAsFactors=FALSE), rxXdfFileName(data), overwrite=TRUE)
}
