#' @include summarise_xdf.R
#' @include summarise_xdf_summary.R
NULL


smryRxSplit <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(length(grps) == 0)
        return(smryRxSummary(data, grps, stats, exprs, rxArgs))

    xdflst <- splitGroups(data)
    outlst <- rxExec(smryRxSummaryWithGroupvars, data=rxElemArg(xdflst), grps, stats, exprs, rxArgs,
        execObjects="deleteIfTbl", packagesToLoad="dplyrXdf")
    combineGroups(outlst, tbl_xdf(data), NULL)
}


smryRxSummaryWithGroupvars <- function(data, grps, stats, exprs, rxArgs)
{
    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
    smry <- smryRxSummary(data, NULL, stats, exprs, rxArgs, dfOut=TRUE)
    cbind(gvars, smry, stringsAsFactors=FALSE)
}


smryRxSplitDplyr <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(length(grps) == 0)
    {
        oldData <- data
        on.exit(deleteIfTbl(oldData))

        cl <- quote(rxDataStep(data, maxRowsByCols=NULL))
        cl[names(rxArgs)] <- rxArgs
        return(eval(cl) %>%
               dplyr::summarise(!!!exprs))
    }

    xdflst <- splitGroups(data)
    outlst <- rxExec(smryDplyrWithGroupvars, data=rxElemArg(xdflst), grps, exprs, rxArgs,
        execObjects="deleteIfTbl", packagesToLoad="dplyr")
    combineGroups(outlst, tbl_xdf(data), NULL)
}


smryDplyrWithGroupvars <- function(data, grps, exprs, rxArgs)
{
    oldData <- data
    on.exit(deleteIfTbl(oldData))

    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
    cl <- quote(rxDataStep(data, maxRowsByCols=NULL))
    cl[names(rxArgs)] <- rxArgs
    #exprs <- lazyeval::as.lazy_dots(exprs, attr(exprs, "env"))
    smry <- eval(cl) %>%
        dplyr::summarise(!!!exprs)
        
    cbind(gvars, smry, stringsAsFactors=FALSE)
}



