#' @include summarise_xdf.R
#' @include summarise_xdf_summary.R
NULL


smryRxSplit <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(is.null(grps))
        return(smry_rxSummary(data, grps, stats, exprs, rxArgs))

    outSource <- tbl(newTbl(data), hasTblFile=TRUE)
    xdflst <- splitGroups(data)
    outlst <- rxExec(smryRxSummaryWithGroupvars, data=rxElemArg(xdflst), grps, stats, exprs, rxArgs,
        execObjects="deleteTbl", packagesToLoad="dplyrXdf")
    combineGroups(outlst, outSource, NULL)
}


smryRxSummaryWithGroupvars <- function(data, grps, stats, exprs, rxArgs)
{
    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
    smry <- smryRxSummary(data, NULL, stats, exprs, rxArgs, dfOut=TRUE)
    cbind(gvars, smry, stringsAsFactors=FALSE)
}


smryRxSplitDplyr <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(is.null(grps))
    {
        oldData <- data
        if(hasTblFile(data))
            on.exit(deleteTbl(oldData))

        cl <- quote(rxDataStep(data, maxRowsByCols=NULL))
        cl[names(rxArgs)] <- rxArgs
        return(eval(cl) %>%
               dplyr::summarise_(.dots=exprs))
    }

    outSource <- tbl(newTbl(data), hasTblFile=TRUE)
    xdflst <- splitGroups(data)
    outlst <- rxExec(smryDplyrWithGroupvars, data=rxElemArg(xdflst), grps, exprs, rxArgs,
        execObjects="deleteTbl", packagesToLoad="dplyr")
    combineGroups(outlst, outSource, NULL)
}


smryDplyrWithGroupvars <- function(data, grps, exprs, rxArgs)
{
    oldData <- data
    if(hasTblFile(data))
        on.exit(deleteTbl(oldData))

    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
    cl <- quote(rxDataStep(data, maxRowsByCols=NULL))
    cl[names(rxArgs)] <- rxArgs
    exprs <- lazyeval::as.lazy_dots(exprs, attr(exprs, "env"))
    smry <- eval(cl) %>%
        dplyr::summarise_(.dots=exprs)
        
    cbind(gvars, smry, stringsAsFactors=FALSE)
}
