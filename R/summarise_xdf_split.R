#' @include summarise_xdf.R
#' @include summarise_xdf_summary.R
NULL


smry_rxSplit <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(is.null(grps))
        return(smry_rxSummary(data, grps, stats, exprs, rxArgs))

    outSource <- tbl(newTbl(data), hasTblFile=TRUE)
    xdflst <- split_groups(data)
    outlst <- rxExec(smry_rxSummary_with_groupvars, data=rxElemArg(xdflst), grps, stats, exprs, rxArgs,
        execObjects="deleteTbl", packagesToLoad="dplyrXdf")
    combine_groups(outlst, outSource, NULL)
}


smry_rxSummary_with_groupvars <- function(data, grps, stats, exprs, rxArgs)
{
    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
    smry <- smry_rxSummary(data, NULL, stats, exprs, rxArgs, dfOut=TRUE)
    cbind(gvars, smry, stringsAsFactors=FALSE)
}


smry_rxSplit_dplyr <- function(data, grps=NULL, stats, exprs, rxArgs)
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
    xdflst <- split_groups(data)
    outlst <- rxExec(smry_dplyr_with_groupvars, data=rxElemArg(xdflst), grps, exprs, rxArgs,
        execObjects="deleteTbl", packagesToLoad="dplyr")
    combine_groups(outlst, outSource, NULL)
}


smry_dplyr_with_groupvars <- function(data, grps, exprs, rxArgs)
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
