#' @include summarise_xdf.R
NULL


smryRxSplit <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(length(grps) == 0)
        return(smryRxSummary(data, grps, stats, exprs, rxArgs))

    outlst <- if(.dxOptions$useExecBy)
        smryExecBy(data, grps, stats, exprs, rxArgs)
    else
    {
        #on.exit(deleteIfTbl(xdflst))
        xdflst <- splitGroups(data)
        rxExec(smryRxSummaryWithGroupvars, data=rxElemArg(xdflst), grps, stats, exprs, rxArgs,
            execObjects=c("smryRxSummary", "buildSmryFormulaRhs"),
            packagesToLoad="dplyr")
    }
    combineGroups(outlst, tbl_xdf(data), NULL)
}


smryRxSummaryWithGroupvars <- function(data, grps, stats, exprs, rxArgs)
{
    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
    smry <- smryRxSummary(data, NULL, stats, exprs, rxArgs)
    cbind(gvars, smry, stringsAsFactors=FALSE)
}


smryExecBy <- function(data, grps, stats, exprs, rxArgs)
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    execByResult(rxExecBy(data, grps, function(keys, data, ...)
        {
            require(dplyr)
            dplyrXdf:::smryRxSummaryWithGroupvars(data, ...)
        },
        list(grps=grps, stats=stats, exprs=exprs, rxArgs=rxArgs)))
}


smryRxSplitDplyr <- function(data, grps=NULL, stats, exprs, rxArgs)
{
    if(length(grps) == 0)
    {
        on.exit(deleteIfTbl(data))

        arglst <- list(data, maxRowsByCols=NULL)
        if(!is.null(rxArgs))
            arglst <- rlang::modify(arglst, !!!rxArgs)
        return(rlang::invoke("rxDataStep", arglst, .env=parent.frame(), .bury=NULL) %>%
            summarise(!!!exprs))
    }

    outlst <- if(.dxOptions$useExecBy)
        smryDplyrExecBy(data, grps, exprs, rxArgs)
    else
    {
        on.exit(deleteIfTbl(xdflst))
        xdflst <- splitGroups(data)
        rxExec(smryDplyrWithGroupvars, data=rxElemArg(xdflst), grps, exprs, rxArgs,
            execObjects="deleteIfTbl", packagesToLoad="dplyr")
    }
    combineGroups(outlst, tbl_xdf(data), NULL)
}


smryDplyrWithGroupvars <- function(data, grps, exprs, rxArgs)
{
    gvars <- rxDataStep(data, varsToKeep=grps, numRows=1)
    arglst <- list(data, maxRowsByCols=NULL)

    if(!is.null(rxArgs))
        arglst <- rlang::modify(arglst, !!!rxArgs)
    smry <- rlang::invoke("rxDataStep", arglst, .env=parent.frame(), .bury=NULL) %>%
        summarise(!!!exprs)
        
    cbind(gvars, smry, stringsAsFactors=FALSE)
}


smryDplyrExecBy <- function(data, grps, exprs, rxArgs)
{
    cc <- rxGetComputeContext()
    on.exit(rxSetComputeContext(cc))

    execByResult(rxExecBy(data, grps, function(keys, data, ...)
        {
            require(dplyr)
            dplyrXdf:::smryDplyrWithGroupvars(data, ...)
        },
        list(grps=grps, exprs=exprs, rxArgs=rxArgs)))
}

